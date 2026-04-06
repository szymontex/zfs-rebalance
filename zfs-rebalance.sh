#!/usr/bin/env bash
# =============================================================================
# ZFS In-Place Rebalancing Tool v3.0.0
# =============================================================================
#
# Redistributes data across ZFS pool vdevs by copying each file to a new
# location on the same filesystem, then atomically replacing the original.
# After rebalancing, new blocks are allocated according to the pool's current
# vdev topology (e.g., after adding a new mirror vdev).
#
# HOW IT WORKS
#
#   For each file in the target directory:
#
#   1. COPY    - cp --reflink=never -ax original original.balance
#   2. SYNC    - sync original.balance  (forces fsync/TXG commit to disk)
#   3. VERIFY  - stat comparison + cmp -s byte-by-byte comparison
#   4. REPLACE - mv -f original.balance original  (atomic rename)
#                sync directory  (force directory entry to disk)
#
#   For hardlink groups: process the main file (lowest path), then
#   ln -f main link for each additional path (atomic via temp+rename).
#
# SAFETY GUARANTEES
#
#   - A file is NEVER deleted without a verified, fsynced copy existing.
#   - Every step is tracked in SQLite with per-file status (8 states).
#   - On crash/kill: recovery on next run uses DB state + disk verification
#     to determine exactly where each file was in the pipeline.
#   - cp does NOT fsync - we call sync explicitly before verification.
#   - mv (rename) is atomic on ZFS (single TXG). We sync the directory
#     afterward to force the TXG to commit (durability guarantee).
#   - ln -f uses linkat+renameat internally - no window where a path
#     is absent. Old inode or new inode, never missing.
#   - All SQL strings escape single quotes to prevent injection.
#   - All file paths are quoted - handles spaces, Polish chars, $, #, etc.
#
# WHAT CAN GO WRONG
#
#   - Power loss during cp: .balance partial. Recovery deletes it, re-copies.
#   - Power loss after cp, before sync: .balance in page cache only.
#     Recovery deletes it (status=copying/syncing -> untrusted).
#   - Power loss during mv: ZFS TXG atomic - either happened or not.
#     Recovery checks inode to determine outcome.
#   - SQLite DB corruption: data files are unaffected. Worst case:
#     some files get re-processed (redundant I/O, no data loss).
#   - File modified during cp: cmp catches mismatch, marks failed.
#
# PARALLEL EXECUTION
#
#   Multiple instances can run simultaneously on different target directories.
#   SQLite WAL mode provides concurrent access. Each instance has its own
#   run_id and temp files (/tmp/zfs-rebalance-PID-*).
#
# LOGGING
#
#   Terminal: colored ANSI output.
#   Log file: SCRIPT_DIR/logs/zfs-rebalance-YYYYMMDD-HHMMSS-PID.log (plain text).
#
#   Progress format:
#     [N/M] (P%) [SIZE] /path   ETA: Xh Ym
#
#   Useful greps:
#     grep "ERR"           logs/*.log    # errors only
#     grep "FAIL:"         logs/*.log    # failed files with reason
#     grep "MANUAL ACTION" logs/*.log    # files needing manual review
#     grep "^\["           logs/*.log    # progress lines
#
# AFTER A CRASH
#
#   Just re-run the script with the same target. It will:
#   1. Detect the crashed run (running status + dead PID)
#   2. Mark it as crashed
#   3. Restore directory mtimes from DB
#   4. Recover each incomplete file based on its status:
#      - pending/copying/syncing: clean up .balance, re-process
#      - verifying: re-verify if both files exist
#      - replacing: check inode to determine if rename completed
#      - relinking: check each hardlink path, recreate missing ones
#   5. Scan for orphaned .balance files not tracked in DB
#   6. Continue with normal processing
#
# USAGE
#
#   # Rebalance a directory:
#   ./zfs-rebalance.sh /mnt/pool/data
#
#   # Dry run - show what would be done:
#   ./zfs-rebalance.sh --dry-run /mnt/pool/data/
#
#   # Check status of all runs:
#   ./zfs-rebalance.sh --status
#
#   # Verify previously rebalanced files:
#   ./zfs-rebalance.sh --verify /mnt/pool/data
#
#   # With checksum verification disabled (faster, stat-only):
#   ./zfs-rebalance.sh -c false /mnt/pool/data/
#
#   # Multiple passes:
#   ./zfs-rebalance.sh -p 2 /mnt/pool/data/
#
#   # Debug mode (verbose output):
#   ./zfs-rebalance.sh --debug /mnt/pool/data/
#
# REQUIREMENTS
#
#   - bash 4+ (associative arrays)
#   - sqlite3 3.24+ (UPSERT support, WAL mode)
#   - GNU coreutils 8.24+ (sync with file argument)
#   - GNU find with -printf
#   - Script directory must be on a DIFFERENT filesystem than ZFS pool
#
# =============================================================================

# ---------------------------------------------------------------------------
# Strict mode
# ---------------------------------------------------------------------------
set -u
set -o pipefail
# NOT set -e: we handle errors explicitly per-command

# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------
readonly VERSION="3.0.0"

# ---------------------------------------------------------------------------
# Bash version check
# ---------------------------------------------------------------------------
if (( BASH_VERSINFO[0] < 4 )); then
    printf "ERROR: bash 4+ required (have %s)\n" "$BASH_VERSION" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly SCRIPT_DIR
readonly DB_PATH="${SCRIPT_DIR}/rebalance.db"
readonly LOG_DIR="${SCRIPT_DIR}/logs"
readonly BATCH_FLUSH_INTERVAL=100
readonly LARGE_FILE_THRESHOLD=1073741824  # 1 GB

# Temp file paths (PID-unique for concurrent access)
readonly TMP_FILES="/tmp/zfs-rebalance-$$-files.txt"
readonly TMP_SORTED="/tmp/zfs-rebalance-$$-sorted.txt"
readonly TMP_GROUPED="/tmp/zfs-rebalance-$$-grouped.txt"
readonly TMP_BATCH="/tmp/zfs-rebalance-$$-batch.sql"

# Colors
readonly C_RESET=$'\033[0m'
readonly C_RED=$'\033[0;31m'
readonly C_GREEN=$'\033[0;32m'
readonly C_YELLOW=$'\033[0;33m'
readonly C_BLUE=$'\033[0;34m'
readonly C_CYAN=$'\033[0;36m'
readonly C_BOLD=$'\033[1m'
readonly C_DIM=$'\033[2m'

# ---------------------------------------------------------------------------
# SQLite Schema
# ---------------------------------------------------------------------------
read -r -d '' SCHEMA << 'SCHEMASQL' || true
PRAGMA busy_timeout = 60000;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS files (
    path        TEXT PRIMARY KEY,
    orig_size   INTEGER NOT NULL,
    orig_mtime  REAL NOT NULL,
    orig_inode  INTEGER NOT NULL,
    new_inode   INTEGER,
    passes      INTEGER NOT NULL DEFAULT 0,
    status      TEXT NOT NULL DEFAULT 'pending'
                CHECK(status IN ('pending','copying','syncing','verifying',
                                 'replacing','relinking','done','failed')),
    link_main   TEXT,
    run_id      INTEGER,
    processed_at TEXT
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    pid         INTEGER NOT NULL,
    target      TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'running'
                CHECK(status IN ('running','completed','interrupted','crashed')),
    started_at  TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    last_file_at TEXT,
    files_ok    INTEGER NOT NULL DEFAULT 0,
    files_fail  INTEGER NOT NULL DEFAULT 0,
    files_skip  INTEGER NOT NULL DEFAULT 0,
    bytes       INTEGER NOT NULL DEFAULT 0,
    duration_s  INTEGER
);

CREATE TABLE IF NOT EXISTS dir_mtimes (
    run_id      INTEGER NOT NULL,
    path        TEXT NOT NULL,
    mtime       REAL NOT NULL,
    PRIMARY KEY (run_id, path)
) WITHOUT ROWID;
SCHEMASQL
readonly SCHEMA

# ---------------------------------------------------------------------------
# State variables
# ---------------------------------------------------------------------------
declare -A file_cache           # path -> passes (for done files)
declare -a pending_batch=()     # SQL statements waiting to be flushed
declare -a failed_files=()      # paths of failed files for summary

run_id=0
interrupted=false
log_file_path=""
use_checksum=true
max_passes=1
no_db=false
skip_orphan_check=false
debug_mode=false
target_path=""

# Counters
files_ok=0
files_fail=0
files_skip=0
bytes_done=0
total_files=0
pre_file_count=0
start_time=0
batch_counter=0

# ---------------------------------------------------------------------------
# SQL Escape - replace single quotes with doubled single quotes
# ---------------------------------------------------------------------------
sql_escape() {
    printf '%s' "${1//\'/\'\'}"
}

# ---------------------------------------------------------------------------
# Format helpers
# ---------------------------------------------------------------------------
format_bytes() {
    local bytes="$1"
    if (( bytes >= 1099511627776 )); then
        printf "%.1fT" "$(echo "scale=1; ${bytes} / 1099511627776" | bc)"
    elif (( bytes >= 1073741824 )); then
        printf "%.1fG" "$(echo "scale=1; ${bytes} / 1073741824" | bc)"
    elif (( bytes >= 1048576 )); then
        printf "%.1fM" "$(echo "scale=1; ${bytes} / 1048576" | bc)"
    elif (( bytes >= 1024 )); then
        printf "%.1fK" "$(echo "scale=1; ${bytes} / 1024" | bc)"
    else
        printf "%dB" "$bytes"
    fi
}

format_duration() {
    local secs="$1"
    local days hours mins
    days=$(( secs / 86400 ))
    secs=$(( secs % 86400 ))
    hours=$(( secs / 3600 ))
    secs=$(( secs % 3600 ))
    mins=$(( secs / 60 ))
    secs=$(( secs % 60 ))

    if (( days > 0 )); then
        printf "%dd %dh %dm %ds" "$days" "$hours" "$mins" "$secs"
    elif (( hours > 0 )); then
        printf "%dh %dm %ds" "$hours" "$mins" "$secs"
    elif (( mins > 0 )); then
        printf "%dm %ds" "$mins" "$secs"
    else
        printf "%ds" "$secs"
    fi
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log_info()  { printf "%s[INFO]%s  %s\n" "$C_BLUE"   "$C_RESET" "$*"; }
log_ok()    { printf "%s[ OK ]%s  %s\n" "$C_GREEN"  "$C_RESET" "$*"; }
log_warn()  { printf "%s[WARN]%s  %s\n" "$C_YELLOW" "$C_RESET" "$*"; }
log_error() { printf "%s[ERR ]%s  %s\n" "$C_RED"    "$C_RESET" "$*"; }
log_debug() {
    if [[ "$debug_mode" == "true" ]]; then
        printf "%s[DBG ]%s  %s\n" "$C_DIM" "$C_RESET" "$*"
    fi
}

# Direct log - bypasses tee, writes to log file directly.
# Used in cleanup when tee may be dead (SSH disconnect kills tee via SIGPIPE).
log_direct() {
    local msg="$1"
    if [[ -n "$log_file_path" ]] && [[ -d "$(dirname "$log_file_path")" ]]; then
        printf "[INFO]  %s\n" "$msg" >> "$log_file_path" 2>/dev/null
    fi
    # Also try stderr as last resort
    printf "[INFO]  %s\n" "$msg" >&2 2>/dev/null || true
}

log_progress() {
    local current="$1"
    local total="$2"
    local file_size="$3"
    local filepath="$4"

    local pct=0
    if (( total > 0 )); then
        pct=$(( current * 100 / total ))
    fi

    local size_str
    size_str=$(format_bytes "$file_size")

    local eta_str=""
    local now
    now=$(date +%s)
    local elapsed=$(( now - start_time ))
    if (( current > 0 && elapsed > 0 )); then
        local remaining_files=$(( total - current ))
        local secs_per_file=$(( elapsed / current ))
        local eta_secs=$(( remaining_files * secs_per_file ))
        eta_str="ETA: $(format_duration "$eta_secs")"
    fi

    printf "%s[%d/%d]%s (%d%%) %s[%s]%s %s   %s\n" \
        "$C_BOLD" "$current" "$total" "$C_RESET" \
        "$pct" \
        "$C_CYAN" "$size_str" "$C_RESET" \
        "$filepath" \
        "$eta_str"
}

# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------
db_init() {
    if [[ "$no_db" == "true" ]]; then
        return 0
    fi
    # Retry init for concurrent startup (two instances creating DB at once)
    local attempt result rc
    for attempt in 1 2 3 4 5; do
        result=$(sqlite3 "$DB_PATH" "$SCHEMA" 2>&1)
        rc=$?
        if (( rc == 0 )); then
            log_debug "SQLite initialized: $DB_PATH"
            return 0
        fi
        if [[ "$result" == *"locked"* || "$result" == *"busy"* ]]; then
            log_debug "DB locked during init, retry $attempt/5..."
            sleep 1
        else
            log_error "Failed to initialize SQLite database at $DB_PATH: $result"
            exit 1
        fi
    done
    log_error "Failed to initialize SQLite after 5 retries: $result"
    exit 1
}

db_exec() {
    if [[ "$no_db" == "true" ]]; then
        return 0
    fi
    # .timeout is the sqlite3 CLI equivalent of PRAGMA busy_timeout
    # but produces NO output (PRAGMA returns the value, polluting results).
    # Must be set per-connection (each sqlite3 call is a new process).
    local result
    result=$(printf '.timeout 60000\n%s\n' "$1" | sqlite3 "$DB_PATH" 2>&1)
    local rc=$?
    if (( rc != 0 )); then
        log_error "SQLite exec failed: $result"
        log_debug "SQL was: $1"
        return 1
    fi
    if [[ -n "$result" ]]; then
        printf '%s' "$result"
    fi
    return 0
}

db_query() {
    if [[ "$no_db" == "true" ]]; then
        return 0
    fi
    # .timeout: no output, sets busy_timeout for this connection
    printf '.timeout 60000\n%s\n' "$1" | sqlite3 "$DB_PATH" 2>&1
}

db_batch_add() {
    local sql="$1"
    pending_batch+=("$sql")
}

db_batch_flush() {
    if [[ "$no_db" == "true" ]]; then
        pending_batch=()
        return 0
    fi
    if (( ${#pending_batch[@]} == 0 )); then
        return 0
    fi

    local batch_sql="BEGIN TRANSACTION;"
    local stmt
    for stmt in "${pending_batch[@]}"; do
        batch_sql+="${stmt}"
    done
    # Update heartbeat
    local escaped_now
    escaped_now=$(date -u +"%Y-%m-%d %H:%M:%S")
    if (( run_id > 0 )); then
        batch_sql+="UPDATE runs SET last_file_at='${escaped_now}', files_ok=${files_ok}, files_fail=${files_fail}, files_skip=${files_skip}, bytes=${bytes_done} WHERE id=${run_id};"
    fi
    batch_sql+="COMMIT;"

    local result
    result=$(printf '.timeout 60000\n%s\n' "$batch_sql" | sqlite3 "$DB_PATH" 2>&1)
    local rc=$?
    if (( rc != 0 )); then
        log_warn "Batch flush failed (${#pending_batch[@]} statements): $result"
        # Try individual statements as fallback
        for stmt in "${pending_batch[@]}"; do
            printf '.timeout 60000\n%s\n' "$stmt" | sqlite3 "$DB_PATH" 2>/dev/null || true
        done
    else
        log_debug "Flushed ${#pending_batch[@]} batch statements"
    fi
    pending_batch=()
    batch_counter=0
}

# ---------------------------------------------------------------------------
# Directory mtime management
# ---------------------------------------------------------------------------
save_dir_mtimes() {
    local target="$1"
    local rid="$2"

    if [[ "$no_db" == "true" ]]; then
        return 0
    fi

    log_info "Saving directory mtimes for crash recovery..."

    local sql="BEGIN TRANSACTION;"
    local count=0
    local dir_mtime dir_path
    while IFS=$'\t' read -r dir_mtime dir_path; do
        [[ -z "$dir_path" ]] && continue
        local esc_path
        esc_path=$(sql_escape "$dir_path")
        sql+="INSERT OR REPLACE INTO dir_mtimes (run_id, path, mtime) VALUES (${rid}, '${esc_path}', ${dir_mtime});"
        (( count++ ))
    done < <(find "$target" -type d -printf '%T@\t%p\n' 2>/dev/null)
    sql+="COMMIT;"

    local result
    result=$(sqlite3 "$DB_PATH" "$sql" 2>&1)
    if (( $? != 0 )); then
        log_warn "Failed to save some directory mtimes: $result"
    else
        log_info "Saved $count directory mtimes"
    fi
}

restore_dir_mtimes() {
    local rid="$1"

    if [[ "$no_db" == "true" ]]; then
        return 0
    fi

    log_info "Restoring directory mtimes for run $rid..."

    local count=0
    while IFS='|' read -r dir_path dir_mtime; do
        [[ -z "$dir_path" ]] && continue
        if [[ -d "$dir_path" ]]; then
            if touch -m -d "@${dir_mtime}" "$dir_path" 2>/dev/null; then
                (( count++ ))
            fi
        fi
    done < <(db_query "SELECT path, mtime FROM dir_mtimes WHERE run_id=${rid};")

    if (( count > 0 )); then
        log_info "Restored $count directory mtimes"
    fi

    # Clean up dir_mtimes records for this run
    db_exec "DELETE FROM dir_mtimes WHERE run_id=${rid};" || true
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print_summary() {
    local now
    now=$(date +%s)
    local elapsed=$(( now - start_time ))
    if (( elapsed < 0 )); then elapsed=0; fi
    local dur_str
    dur_str=$(format_duration "$elapsed")

    printf "\n%s========================================%s\n" "$C_BOLD" "$C_RESET"
    printf "%s  REBALANCE SUMMARY%s\n" "$C_BOLD" "$C_RESET"
    printf "%s========================================%s\n" "$C_BOLD" "$C_RESET"
    printf "  Duration:      %s\n" "$dur_str"
    printf "  Rebalanced:    %d files (%s)\n" "$files_ok" "$(format_bytes "$bytes_done")"
    printf "  Skipped:       %d files\n" "$files_skip"
    printf "  Failed:        %d files\n" "$files_fail"

    if (( elapsed > 0 && bytes_done > 0 )); then
        local throughput=$(( bytes_done / elapsed ))
        printf "  Throughput:    %s/s\n" "$(format_bytes "$throughput")"
    fi

    # Post-run file count check
    if (( pre_file_count > 0 )) && [[ -n "$target_path" ]] && [[ -d "$target_path" ]]; then
        local post_count
        post_count=$(find "$target_path" -type f ! -name "*.balance" 2>/dev/null | wc -l)
        post_count=$(( post_count + 0 ))
        if (( post_count == pre_file_count )); then
            printf "  %sFile count:    %d (matches pre-run)%s\n" "$C_GREEN" "$post_count" "$C_RESET"
        else
            printf "  %sFile count:    %d (pre-run: %d) - MISMATCH!%s\n" "$C_RED" "$post_count" "$pre_file_count" "$C_RESET"
        fi
    fi

    # Failed files list
    if (( ${#failed_files[@]} > 0 )); then
        printf "\n  %sFailed files:%s\n" "$C_RED" "$C_RESET"
        local ff
        for ff in "${failed_files[@]}"; do
            printf "    %s\n" "$ff"
        done
    fi

    printf "%s========================================%s\n\n" "$C_BOLD" "$C_RESET"
}

# ---------------------------------------------------------------------------
# Cleanup and signal handling
# ---------------------------------------------------------------------------
cleanup() {
    local exit_code=${1:-$?}

    # Prevent re-entrancy
    trap '' EXIT INT TERM HUP

    if [[ "$interrupted" == "true" ]]; then
        log_direct "Interrupted - performing cleanup..."
    fi

    # Best-effort batch flush
    if (( ${#pending_batch[@]} > 0 )); then
        log_direct "Flushing ${#pending_batch[@]} pending batch statements..."
        db_batch_flush 2>/dev/null || true
    fi

    # Update run status
    if (( run_id > 0 )); then
        local final_status="completed"
        if [[ "$interrupted" == "true" ]]; then
            final_status="interrupted"
        fi
        local now
        now=$(date -u +"%Y-%m-%d %H:%M:%S")
        local elapsed=$(( $(date +%s) - start_time ))
        if (( elapsed < 0 )); then elapsed=0; fi
        db_exec "UPDATE runs SET status='${final_status}', finished_at='${now}', files_ok=${files_ok}, files_fail=${files_fail}, files_skip=${files_skip}, bytes=${bytes_done}, duration_s=${elapsed} WHERE id=${run_id};" 2>/dev/null || true

        # Restore dir mtimes
        restore_dir_mtimes "$run_id" 2>/dev/null || true
    fi

    # Clean temp files
    rm -f "$TMP_FILES" "$TMP_SORTED" "$TMP_GROUPED" "$TMP_BATCH" 2>/dev/null || true

    # Print summary if we actually processed files
    if (( total_files > 0 || files_ok > 0 || files_fail > 0 )); then
        print_summary 2>/dev/null || true
    fi

    exit "$exit_code"
}

handle_signal() {
    interrupted=true
    log_direct "Signal received - shutting down gracefully..."
    exit 130
}

trap cleanup EXIT
trap handle_signal INT TERM HUP

# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------
verify_copy() {
    local original="$1"
    local copy="$2"

    # Compare stat metadata: permissions, owner, group, size, mtime
    local orig_meta copy_meta
    orig_meta=$(stat -c "%a %u %g %s %Y" "$original" 2>/dev/null) || return 1
    copy_meta=$(stat -c "%a %u %g %s %Y" "$copy" 2>/dev/null) || return 1

    if [[ "$orig_meta" != "$copy_meta" ]]; then
        log_debug "Metadata mismatch: original=[$orig_meta] copy=[$copy_meta]"
        return 1
    fi

    # Byte-by-byte content comparison (if enabled)
    if [[ "$use_checksum" == "true" ]]; then
        if ! cmp -s "$original" "$copy"; then
            log_debug "Content mismatch (cmp -s failed)"
            return 1
        fi
    fi

    return 0
}

# ---------------------------------------------------------------------------
# Recovery system
# ---------------------------------------------------------------------------
recover_crashed_runs() {
    if [[ "$no_db" == "true" ]]; then
        return 0
    fi

    local crashed_runs
    crashed_runs=$(db_query "SELECT id, pid, target FROM runs WHERE status='running';")
    [[ -z "$crashed_runs" ]] && return 0

    while IFS='|' read -r rid rpid rtarget; do
        [[ -z "$rid" ]] && continue

        # Skip our own run (just registered)
        if (( rid == run_id )); then
            continue
        fi

        # Check if PID is still alive
        if kill -0 "$rpid" 2>/dev/null; then
            log_info "Run $rid (PID $rpid) is still alive on target: $rtarget"
            continue
        fi

        log_warn "Found crashed run $rid (PID $rpid was dead) on target: $rtarget"

        # Mark as crashed
        db_exec "UPDATE runs SET status='crashed', finished_at=datetime('now') WHERE id=${rid};"

        # Restore dir mtimes for crashed run
        restore_dir_mtimes "$rid"

        # Recover incomplete files
        recover_incomplete_files "$rid"
    done <<< "$crashed_runs"
}

recover_incomplete_files() {
    local rid="$1"

    local incomplete
    incomplete=$(db_query "SELECT path, status, orig_size, orig_mtime, orig_inode, link_main FROM files WHERE run_id=${rid} AND status NOT IN ('done','failed');")
    [[ -z "$incomplete" ]] && return 0

    local recovered=0
    local failed=0

    while IFS='|' read -r fpath fstatus forig_size forig_mtime forig_inode flink_main; do
        [[ -z "$fpath" ]] && continue

        local balance_file="${fpath}.balance"
        local esc_path
        esc_path=$(sql_escape "$fpath")

        log_debug "Recovering: status=$fstatus path=$fpath"

        case "$fstatus" in
            pending)
                # Nothing happened to this file on disk
                if [[ -f "$fpath" ]]; then
                    local cur_size cur_mtime
                    cur_size=$(stat -c %s "$fpath" 2>/dev/null) || cur_size=""
                    cur_mtime=$(stat -c %Y "$fpath" 2>/dev/null) || cur_mtime=""
                    if [[ "$cur_size" == "$forig_size" && "$cur_mtime" == "$forig_mtime" ]]; then
                        # Original unchanged - delete record, will re-process
                        db_exec "DELETE FROM files WHERE path='${esc_path}';"
                        (( recovered++ ))
                    else
                        log_warn "RECOVERY: Original changed since pending: $fpath"
                        db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_path}';"
                        (( failed++ ))
                    fi
                else
                    log_error "RECOVERY ALARM: Original missing for pending file: $fpath"
                    db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_path}';"
                    (( failed++ ))
                fi
                ;;

            copying|syncing)
                # .balance is UNTRUSTED - always delete it
                if [[ -f "$balance_file" ]]; then
                    rm -f "$balance_file"
                    log_info "RECOVERY: Deleted untrusted .balance: $balance_file"
                fi
                if [[ -f "$fpath" ]]; then
                    local cur_size cur_mtime
                    cur_size=$(stat -c %s "$fpath" 2>/dev/null) || cur_size=""
                    cur_mtime=$(stat -c %Y "$fpath" 2>/dev/null) || cur_mtime=""
                    if [[ "$cur_size" == "$forig_size" && "$cur_mtime" == "$forig_mtime" ]]; then
                        # Original intact - delete record, re-process next run
                        db_exec "DELETE FROM files WHERE path='${esc_path}';"
                        (( recovered++ ))
                    else
                        log_warn "RECOVERY: Original changed during $fstatus: $fpath"
                        db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_path}';"
                        (( failed++ ))
                    fi
                else
                    log_error "RECOVERY ALARM: Original missing for $fstatus file: $fpath"
                    db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_path}';"
                    (( failed++ ))
                fi
                ;;

            verifying)
                # .balance is fsynced but verification didn't finish
                if [[ -f "$balance_file" ]] && [[ -f "$fpath" ]]; then
                    # Re-verify (read-only, safe)
                    log_info "RECOVERY: Re-verifying: $fpath"
                    if verify_copy "$fpath" "$balance_file"; then
                        # Verification passed - proceed to replace
                        log_info "RECOVERY: Re-verification passed, replacing: $fpath"
                        db_exec "UPDATE files SET status='replacing' WHERE path='${esc_path}';"
                        if mv -f "$balance_file" "$fpath" 2>/dev/null; then
                            sync "$(dirname "$fpath")" 2>/dev/null || true
                            local new_inode
                            new_inode=$(stat -c %i "$fpath" 2>/dev/null) || new_inode=0
                            db_exec "UPDATE files SET status='done', new_inode=${new_inode}, passes=passes+1, processed_at=datetime('now') WHERE path='${esc_path}';"
                            (( recovered++ ))
                        else
                            log_error "RECOVERY: mv failed during re-verification recovery: $fpath"
                            log_error "MANUAL ACTION: mv -f '${balance_file}' '${fpath}'"
                            db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_path}';"
                            (( failed++ ))
                        fi
                    else
                        # Verification failed - delete .balance
                        rm -f "$balance_file"
                        log_warn "RECOVERY: Re-verification failed, deleted .balance: $fpath"
                        db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_path}';"
                        (( failed++ ))
                    fi
                elif [[ -f "$balance_file" ]] && [[ ! -f "$fpath" ]]; then
                    # Original missing, .balance exists - ALARM, preserve .balance
                    log_error "RECOVERY ALARM: Original vanished. .balance preserved at: $balance_file"
                    log_error "MANUAL ACTION: Verify .balance integrity, then: mv '${balance_file}' '${fpath}'"
                    db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_path}';"
                    (( failed++ ))
                elif [[ ! -f "$balance_file" ]] && [[ -f "$fpath" ]]; then
                    # .balance gone, original exists - delete record, re-process
                    db_exec "DELETE FROM files WHERE path='${esc_path}';"
                    (( recovered++ ))
                else
                    # Both missing - CRITICAL
                    log_error "RECOVERY CRITICAL: Both original and .balance missing: $fpath"
                    db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_path}';"
                    (( failed++ ))
                fi
                ;;

            replacing)
                # rename() was in progress - atomic on ZFS (single TXG)
                if [[ -f "$fpath" ]]; then
                    local cur_inode
                    cur_inode=$(stat -c %i "$fpath" 2>/dev/null) || cur_inode=0
                    if [[ "$cur_inode" != "$forig_inode" ]]; then
                        # Rename HAPPENED (different inode)
                        if [[ -f "$balance_file" ]]; then
                            log_warn "RECOVERY: .balance still exists after completed rename: $balance_file (removing)"
                            rm -f "$balance_file"
                        fi
                        # Check and fix hardlink group members if this is a leader
                        recover_relinks_if_needed "$fpath" "$esc_path" "$cur_inode"
                        db_exec "UPDATE files SET status='done', new_inode=${cur_inode}, passes=passes+1, processed_at=datetime('now') WHERE path='${esc_path}';"
                        (( recovered++ ))
                    else
                        # Rename DID NOT happen (same inode)
                        if [[ -f "$balance_file" ]]; then
                            # Re-verify and retry
                            log_info "RECOVERY: Rename didn't happen, re-verifying: $fpath"
                            if verify_copy "$fpath" "$balance_file"; then
                                if mv -f "$balance_file" "$fpath" 2>/dev/null; then
                                    sync "$(dirname "$fpath")" 2>/dev/null || true
                                    local new_inode
                                    new_inode=$(stat -c %i "$fpath" 2>/dev/null) || new_inode=0
                                    recover_relinks_if_needed "$fpath" "$esc_path" "$new_inode"
                                    db_exec "UPDATE files SET status='done', new_inode=${new_inode}, passes=passes+1, processed_at=datetime('now') WHERE path='${esc_path}';"
                                    (( recovered++ ))
                                else
                                    log_error "RECOVERY: mv failed: $fpath"
                                    log_error "MANUAL ACTION: mv -f '${balance_file}' '${fpath}'"
                                    db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_path}';"
                                    (( failed++ ))
                                fi
                            else
                                # Re-verification failed
                                rm -f "$balance_file"
                                db_exec "DELETE FROM files WHERE path='${esc_path}';"
                                (( recovered++ ))
                            fi
                        else
                            # No .balance, same inode - nothing happened, delete record
                            db_exec "DELETE FROM files WHERE path='${esc_path}';"
                            (( recovered++ ))
                        fi
                    fi
                else
                    # Original missing during replacing - should not happen with rename()
                    log_error "RECOVERY CRITICAL: Original missing during replacing status: $fpath"
                    if [[ -f "$balance_file" ]]; then
                        log_error "MANUAL ACTION: mv '${balance_file}' '${fpath}'"
                    fi
                    db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_path}';"
                    (( failed++ ))
                fi
                ;;

            relinking)
                # Main file was already replaced, hardlinks being updated
                recover_relinking "$fpath" "$esc_path" "$flink_main"
                (( recovered++ ))
                ;;

            *)
                log_warn "RECOVERY: Unknown status '$fstatus' for: $fpath"
                db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_path}';"
                (( failed++ ))
                ;;
        esac
    done <<< "$incomplete"

    if (( recovered > 0 || failed > 0 )); then
        log_info "Recovery for run $rid complete: $recovered recovered, $failed failed"
    fi
}

recover_relinks_if_needed() {
    local main_path="$1"
    local esc_main="$2"
    local main_inode="$3"

    # Check if this file is a hardlink group leader
    local group_members
    group_members=$(db_query "SELECT path FROM files WHERE link_main='${esc_main}' AND path != '${esc_main}';")
    [[ -z "$group_members" ]] && return 0

    while IFS= read -r link_path; do
        [[ -z "$link_path" ]] && continue
        local esc_link
        esc_link=$(sql_escape "$link_path")

        if [[ -f "$link_path" ]]; then
            local link_inode
            link_inode=$(stat -c %i "$link_path" 2>/dev/null) || link_inode=0
            if [[ "$link_inode" == "$main_inode" ]]; then
                # Already correct
                db_exec "UPDATE files SET status='done', new_inode=${main_inode}, passes=passes+1, processed_at=datetime('now') WHERE path='${esc_link}';"
            else
                # Needs relinking
                if ln -f "$main_path" "$link_path" 2>/dev/null; then
                    db_exec "UPDATE files SET status='done', new_inode=${main_inode}, passes=passes+1, processed_at=datetime('now') WHERE path='${esc_link}';"
                    log_info "RECOVERY: Relinked: $link_path"
                else
                    log_error "RECOVERY: ln -f failed: $link_path"
                    log_error "MANUAL ACTION: ln -f '${main_path}' '${link_path}'"
                    db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_link}';"
                fi
            fi
        else
            # Path missing - recreate from main
            if ln -f "$main_path" "$link_path" 2>/dev/null; then
                db_exec "UPDATE files SET status='done', new_inode=${main_inode}, passes=passes+1, processed_at=datetime('now') WHERE path='${esc_link}';"
                log_info "RECOVERY: Recreated missing link: $link_path"
            else
                log_error "RECOVERY: Cannot recreate link: $link_path"
                log_error "MANUAL ACTION: ln -f '${main_path}' '${link_path}'"
                db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_link}';"
            fi
        fi
    done <<< "$group_members"
}

recover_relinking() {
    local fpath="$1"
    local esc_path="$2"
    local flink_main="$3"

    if [[ -n "$flink_main" ]]; then
        # This is a group member, not the leader
        local main_path="$flink_main"
        if [[ -f "$main_path" ]]; then
            local main_inode
            main_inode=$(stat -c %i "$main_path" 2>/dev/null) || main_inode=0

            if [[ -f "$fpath" ]]; then
                local cur_inode
                cur_inode=$(stat -c %i "$fpath" 2>/dev/null) || cur_inode=0
                if [[ "$cur_inode" == "$main_inode" ]]; then
                    # Already correct
                    db_exec "UPDATE files SET status='done', new_inode=${main_inode}, passes=passes+1, processed_at=datetime('now') WHERE path='${esc_path}';"
                else
                    # Needs relinking
                    if ln -f "$main_path" "$fpath" 2>/dev/null; then
                        local new_inode
                        new_inode=$(stat -c %i "$fpath" 2>/dev/null) || new_inode=0
                        db_exec "UPDATE files SET status='done', new_inode=${new_inode}, passes=passes+1, processed_at=datetime('now') WHERE path='${esc_path}';"
                        log_info "RECOVERY: Relinked group member: $fpath"
                    else
                        log_error "RECOVERY: ln -f failed for group member: $fpath"
                        log_error "MANUAL ACTION: ln -f '${main_path}' '${fpath}'"
                        db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_path}';"
                    fi
                fi
            else
                # Path missing - recreate from main
                if ln -f "$main_path" "$fpath" 2>/dev/null; then
                    local new_inode
                    new_inode=$(stat -c %i "$fpath" 2>/dev/null) || new_inode=0
                    db_exec "UPDATE files SET status='done', new_inode=${new_inode}, passes=passes+1, processed_at=datetime('now') WHERE path='${esc_path}';"
                    log_info "RECOVERY: Recreated missing link from main: $fpath"
                else
                    log_error "RECOVERY: Cannot recreate link from main: $fpath"
                    log_error "MANUAL ACTION: ln -f '${main_path}' '${fpath}'"
                    db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_path}';"
                fi
            fi
        else
            log_error "RECOVERY ALARM: Main file missing for relinking group member: $fpath (main: $main_path)"
            db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_path}';"
        fi
    else
        # This is the group leader in relinking status
        if [[ -f "$fpath" ]]; then
            local main_inode
            main_inode=$(stat -c %i "$fpath" 2>/dev/null) || main_inode=0
            recover_relinks_if_needed "$fpath" "$esc_path" "$main_inode"
            db_exec "UPDATE files SET status='done', new_inode=${main_inode}, passes=passes+1, processed_at=datetime('now') WHERE path='${esc_path}';"
        else
            log_error "RECOVERY CRITICAL: Group leader missing during relinking: $fpath"
            db_exec "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_path}';"
        fi
    fi
}

check_orphaned_balance_files() {
    local target="$1"

    if [[ "$skip_orphan_check" == "true" ]]; then
        log_info "Skipping orphan .balance file check (--skip-orphan-check)"
        return 0
    fi

    log_info "Scanning for orphaned .balance files..."

    local orphan_count=0
    while IFS= read -r -d '' balance_file; do
        [[ -z "$balance_file" ]] && continue

        local orig_path="${balance_file%.balance}"
        local esc_orig
        esc_orig=$(sql_escape "$orig_path")

        # Check if tracked in DB
        local db_status=""
        if [[ "$no_db" != "true" ]]; then
            db_status=$(db_query "SELECT status FROM files WHERE path='${esc_orig}';")
        fi

        if [[ -z "$db_status" ]]; then
            # Untracked orphan - never auto-fix
            (( orphan_count++ ))
            if [[ -f "$orig_path" ]]; then
                log_warn "Orphan .balance (original exists): $balance_file"
                log_warn "  Suggest: rm '${balance_file}'"
            else
                log_error "Orphan .balance (original MISSING): $balance_file"
                log_error "  MANUAL ACTION: Verify integrity, then: mv '${balance_file}' '${orig_path}'"
            fi
        fi
    done < <(find "$target" -name "*.balance" -type f -print0 2>/dev/null)

    if (( orphan_count > 0 )); then
        log_warn "Found $orphan_count orphaned .balance files (not auto-fixed)"
    else
        log_info "No orphaned .balance files found"
    fi
}

# ---------------------------------------------------------------------------
# Core file processing pipeline
# ---------------------------------------------------------------------------
process_single_file() {
    local main_file="$1"
    local link_main="${2:-}"  # empty for standalone, main path for group members

    # Pre-check: file must exist
    if [[ ! -f "$main_file" ]]; then
        log_debug "File vanished before processing: $main_file"
        (( files_skip++ ))
        return 0
    fi

    local tmp_file="${main_file}.balance"

    # Pre-check: leftover .balance from outside DB tracking
    if [[ -f "$tmp_file" ]]; then
        log_warn "Leftover .balance found (deleting): $tmp_file"
        rm -f "$tmp_file"
    fi

    # Phase 0: RECORD intent - stat original before touching anything
    local orig_size orig_mtime orig_inode
    orig_size=$(stat -c %s "$main_file" 2>/dev/null) || {
        log_warn "stat failed (file vanished): $main_file"
        (( files_skip++ ))
        return 0
    }
    orig_mtime=$(stat -c %Y "$main_file" 2>/dev/null) || {
        log_warn "stat mtime failed: $main_file"
        (( files_skip++ ))
        return 0
    }
    orig_inode=$(stat -c %i "$main_file" 2>/dev/null) || {
        log_warn "stat inode failed: $main_file"
        (( files_skip++ ))
        return 0
    }

    local esc_path
    esc_path=$(sql_escape "$main_file")

    local link_main_sql="NULL"
    if [[ -n "$link_main" ]]; then
        local esc_link_main
        esc_link_main=$(sql_escape "$link_main")
        link_main_sql="'${esc_link_main}'"
    fi

    # INSERT into DB with status=pending
    if [[ "$no_db" != "true" ]]; then
        db_exec "INSERT OR REPLACE INTO files (path, orig_size, orig_mtime, orig_inode, status, link_main, run_id, passes, new_inode, processed_at) VALUES ('${esc_path}', ${orig_size}, ${orig_mtime}, ${orig_inode}, 'pending', ${link_main_sql}, ${run_id}, COALESCE((SELECT passes FROM files WHERE path='${esc_path}'), 0), NULL, NULL);" || {
            log_error "DB INSERT failed: $main_file"
            (( files_fail++ ))
            failed_files+=("$main_file")
            return 1
        }
    fi

    # Phase 1: COPY
    if [[ "$no_db" != "true" ]]; then
        db_exec "UPDATE files SET status='copying' WHERE path='${esc_path}';" || true
    fi

    local cp_output
    cp_output=$(cp --reflink=never -ax "$main_file" "$tmp_file" 2>&1)
    local cp_rc=$?
    if (( cp_rc != 0 )); then
        log_error "FAIL: cp failed (rc=$cp_rc): $main_file"
        log_error "  cp output: $cp_output"
        rm -f "$tmp_file" 2>/dev/null || true
        if [[ "$no_db" != "true" ]]; then
            db_batch_add "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='$(sql_escape "$main_file")';"
        fi
        (( files_fail++ ))
        failed_files+=("$main_file")
        return 1
    fi

    # Phase 2: SYNC (fsync barrier - forces data to physical disk)
    # cp does NOT call fsync. Data is in page cache only.
    # Without sync, power loss after cmp-says-OK means data loss.
    if [[ "$no_db" != "true" ]]; then
        db_exec "UPDATE files SET status='syncing' WHERE path='${esc_path}';" || true
    fi

    if ! sync "$tmp_file" 2>/dev/null; then
        log_error "FAIL: sync failed: $tmp_file"
        rm -f "$tmp_file" 2>/dev/null || true
        if [[ "$no_db" != "true" ]]; then
            db_batch_add "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='$(sql_escape "$main_file")';"
        fi
        (( files_fail++ ))
        failed_files+=("$main_file")
        return 1
    fi

    # Phase 3: VERIFY
    if [[ "$no_db" != "true" ]]; then
        db_exec "UPDATE files SET status='verifying' WHERE path='${esc_path}';" || true
    fi

    # Check original still exists (could have been deleted by another process)
    if [[ ! -f "$main_file" ]]; then
        # ALARM: original vanished. DO NOT delete .balance - it may be the only copy.
        log_error "FAIL: Original vanished after copy. .balance preserved: $tmp_file"
        log_error "MANUAL ACTION: Verify .balance integrity, then: mv '${tmp_file}' '${main_file}'"
        if [[ "$no_db" != "true" ]]; then
            db_batch_add "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='$(sql_escape "$main_file")';"
        fi
        (( files_fail++ ))
        failed_files+=("$main_file")
        return 1
    fi

    if ! verify_copy "$main_file" "$tmp_file"; then
        log_error "FAIL: Verification failed: $main_file"
        rm -f "$tmp_file"
        if [[ "$no_db" != "true" ]]; then
            db_batch_add "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='$(sql_escape "$main_file")';"
        fi
        (( files_fail++ ))
        failed_files+=("$main_file")
        return 1
    fi

    # Phase 4: REPLACE (atomic rename - single rename() syscall)
    if [[ "$no_db" != "true" ]]; then
        db_exec "UPDATE files SET status='replacing' WHERE path='${esc_path}';" || true
    fi

    if ! mv -f "$tmp_file" "$main_file" 2>/dev/null; then
        log_error "FAIL: mv failed: $main_file"
        log_error "MANUAL ACTION: mv -f '${tmp_file}' '${main_file}'"
        if [[ "$no_db" != "true" ]]; then
            db_batch_add "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='$(sql_escape "$main_file")';"
        fi
        (( files_fail++ ))
        failed_files+=("$main_file")
        return 1
    fi

    # Sync directory to force rename TXG to commit (durability guarantee)
    sync "$(dirname "$main_file")" 2>/dev/null || true

    # Phase 5: DONE
    local new_inode
    new_inode=$(stat -c %i "$main_file" 2>/dev/null) || {
        # Should be impossible - we just mv'd to this path
        log_error "FAIL: stat on rebalanced file failed (impossible): $main_file"
        if [[ "$no_db" != "true" ]]; then
            db_batch_add "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='$(sql_escape "$main_file")';"
        fi
        (( files_fail++ ))
        failed_files+=("$main_file")
        return 1
    }

    if [[ "$no_db" != "true" ]]; then
        db_batch_add "UPDATE files SET status='done', new_inode=${new_inode}, passes=passes+1, processed_at=datetime('now') WHERE path='$(sql_escape "$main_file")';"
    fi

    (( files_ok++ ))
    bytes_done=$(( bytes_done + orig_size ))

    return 0
}

process_inode_group() {
    local -a paths=("$@")
    local num_paths=${#paths[@]}

    if (( num_paths == 0 )); then
        return 0
    fi

    # Sort paths to get deterministic main file (lowest lexicographic path)
    local -a sorted_paths
    local sorted_output
    sorted_output=$(printf '%s\n' "${paths[@]}" | sort)
    local old_ifs="$IFS"
    IFS=$'\n'
    # shellcheck disable=SC2206
    sorted_paths=($sorted_output)
    IFS="$old_ifs"

    local main_file="${sorted_paths[0]}"

    # Check if already done (from file_cache)
    if [[ -n "${file_cache["$main_file"]+isset}" ]]; then
        local cached_passes="${file_cache["$main_file"]}"
        if (( cached_passes >= max_passes )); then
            log_debug "Skip (already done, pass $cached_passes/$max_passes): $main_file"
            (( files_skip++ ))
            # Also skip group members
            local i
            for (( i = 1; i < ${#sorted_paths[@]}; i++ )); do
                if [[ -n "${file_cache["${sorted_paths[$i]}"]+isset}" ]]; then
                    local link_passes="${file_cache["${sorted_paths[$i]}"]}"
                    if (( link_passes >= max_passes )); then
                        (( files_skip++ ))
                    fi
                fi
            done
            return 0
        fi
    fi

    local file_size
    file_size=$(stat -c %s "$main_file" 2>/dev/null) || file_size=0

    # Log progress
    local current_count=$(( files_ok + files_fail + files_skip ))
    if (( total_files > 0 )); then
        log_progress "$current_count" "$total_files" "$file_size" "$main_file"
    fi

    if (( ${#sorted_paths[@]} == 1 )); then
        # Standalone file - simple processing
        process_single_file "$main_file" ""
    else
        # Hardlink group - process main file first, then relink others
        process_single_file "$main_file" ""
        local main_rc=$?

        if (( main_rc == 0 )); then
            # Main file successfully rebalanced - now relink all other paths
            local main_inode
            main_inode=$(stat -c %i "$main_file" 2>/dev/null) || main_inode=0
            local esc_main
            esc_main=$(sql_escape "$main_file")

            # Record group members in DB with status=relinking
            if [[ "$no_db" != "true" ]]; then
                local link_insert_sql=""
                local i
                for (( i = 1; i < ${#sorted_paths[@]}; i++ )); do
                    local link_path="${sorted_paths[$i]}"
                    local esc_link
                    esc_link=$(sql_escape "$link_path")

                    # Get original metadata for the link
                    local link_size link_mtime link_inode
                    link_size=$(stat -c %s "$link_path" 2>/dev/null) || link_size=0
                    link_mtime=$(stat -c %Y "$link_path" 2>/dev/null) || link_mtime=0
                    link_inode=$(stat -c %i "$link_path" 2>/dev/null) || link_inode=0

                    link_insert_sql+="INSERT OR REPLACE INTO files (path, orig_size, orig_mtime, orig_inode, status, link_main, run_id, passes, new_inode, processed_at) VALUES ('${esc_link}', ${link_size}, ${link_mtime}, ${link_inode}, 'relinking', '${esc_main}', ${run_id}, COALESCE((SELECT passes FROM files WHERE path='${esc_link}'), 0), NULL, NULL);"
                done
                if [[ -n "$link_insert_sql" ]]; then
                    db_exec "$link_insert_sql" || true
                fi
            fi

            # Atomically relink each path
            local i
            for (( i = 1; i < ${#sorted_paths[@]}; i++ )); do
                local link_path="${sorted_paths[$i]}"

                # Check if link already done in cache
                if [[ -n "${file_cache["$link_path"]+isset}" ]]; then
                    local link_passes="${file_cache["$link_path"]}"
                    if (( link_passes >= max_passes )); then
                        (( files_skip++ ))
                        continue
                    fi
                fi

                # Check if link needs updating (may already have correct inode)
                local link_inode
                link_inode=$(stat -c %i "$link_path" 2>/dev/null) || link_inode=0

                if [[ "$link_inode" != "$main_inode" ]] || [[ ! -e "$link_path" ]]; then
                    # ln -f: atomic via linkat(temp) + renameat(temp, dest)
                    # At no point does link_path not exist.
                    if ln -f "$main_file" "$link_path" 2>/dev/null; then
                        log_debug "Relinked: $link_path -> $main_file"
                        if [[ "$no_db" != "true" ]]; then
                            local esc_link
                            esc_link=$(sql_escape "$link_path")
                            db_batch_add "UPDATE files SET status='done', new_inode=${main_inode}, passes=passes+1, processed_at=datetime('now') WHERE path='${esc_link}';"
                        fi
                        (( files_ok++ ))
                    else
                        log_error "FAIL: ln -f failed: $link_path"
                        log_error "MANUAL ACTION: ln -f '${main_file}' '${link_path}'"
                        if [[ "$no_db" != "true" ]]; then
                            local esc_link
                            esc_link=$(sql_escape "$link_path")
                            db_batch_add "UPDATE files SET status='failed', processed_at=datetime('now') WHERE path='${esc_link}';"
                        fi
                        (( files_fail++ ))
                        failed_files+=("$link_path")
                    fi
                else
                    # Already has the correct inode
                    if [[ "$no_db" != "true" ]]; then
                        local esc_link
                        esc_link=$(sql_escape "$link_path")
                        db_batch_add "UPDATE files SET status='done', new_inode=${main_inode}, passes=passes+1, processed_at=datetime('now') WHERE path='${esc_link}';"
                    fi
                    (( files_ok++ ))
                fi
            done
        fi
    fi

    # Batch flush logic
    (( batch_counter++ ))
    if (( batch_counter >= BATCH_FLUSH_INTERVAL )) || (( file_size > LARGE_FILE_THRESHOLD )); then
        db_batch_flush
    fi
}

# ---------------------------------------------------------------------------
# File enumeration
# ---------------------------------------------------------------------------
enumerate_files() {
    local target="$1"

    log_info "Enumerating files in: $target"

    # find -printf: inode, size, path (tab-separated)
    # Exclude .balance files (leftover temps)
    find "$target" -type f ! -name "*.balance" -printf '%i\t%s\t%p\n' > "$TMP_FILES" 2>/dev/null
    local find_rc=$?
    if (( find_rc != 0 )); then
        log_warn "find returned non-zero ($find_rc) - some files may be inaccessible"
    fi

    # Count files
    total_files=$(wc -l < "$TMP_FILES" 2>/dev/null)
    total_files=$(( total_files + 0 ))  # strip whitespace / ensure numeric

    if (( total_files == 0 )); then
        log_info "No files found in target directory"
        return 1
    fi

    # Pre-run stats
    local total_bytes largest_file
    total_bytes=$(awk -F'\t' '{s+=$2} END {print s+0}' "$TMP_FILES")
    largest_file=$(awk -F'\t' 'BEGIN{m=0} {if($2>m) m=$2} END {print m+0}' "$TMP_FILES")

    pre_file_count=$total_files

    log_info "Files found: $total_files"
    log_info "Total size: $(format_bytes "$total_bytes")"
    log_info "Largest file: $(format_bytes "$largest_file")"

    # Free space check
    local free_space
    free_space=$(df --output=avail -B1 "$target" 2>/dev/null | tail -1 | tr -d ' ')
    if [[ -n "$free_space" ]] && (( free_space > 0 )); then
        log_info "Free space: $(format_bytes "$free_space")"
        if (( largest_file > free_space )); then
            log_error "Insufficient free space! Largest file ($(format_bytes "$largest_file")) > free space ($(format_bytes "$free_space"))"
            return 1
        fi
    fi

    # Sort by inode for hardlink grouping
    sort -t$'\t' -k1,1n "$TMP_FILES" > "$TMP_SORTED"

    # Group by inode: each line = inode TAB path1 TAB path2 TAB ...
    awk -F'\t' '
    {
        inode = $1
        path = $3
        if (inode == prev_inode) {
            printf "\t%s", path
        } else {
            if (NR > 1) printf "\n"
            printf "%s\t%s", inode, path
            prev_inode = inode
        }
    }
    END { if (NR > 0) printf "\n" }
    ' "$TMP_SORTED" > "$TMP_GROUPED"

    local group_count
    group_count=$(wc -l < "$TMP_GROUPED" | tr -d ' ')
    log_info "Inode groups: $group_count"

    return 0
}

# ---------------------------------------------------------------------------
# Load file cache (already-done files from DB)
# ---------------------------------------------------------------------------
load_file_cache() {
    local target="$1"

    if [[ "$no_db" == "true" ]]; then
        return 0
    fi

    log_info "Loading processed files from database..."

    local esc_target
    esc_target=$(sql_escape "$target")

    local count=0
    while IFS='|' read -r fpath fpasses; do
        [[ -z "$fpath" ]] && continue
        file_cache["$fpath"]="$fpasses"
        (( count++ ))
    done < <(db_query "SELECT path, passes FROM files WHERE status='done' AND path LIKE '${esc_target}%';")

    log_info "Loaded $count previously processed files"
}

# ---------------------------------------------------------------------------
# Command: --status
# ---------------------------------------------------------------------------
cmd_status() {
    local target="${1:-}"

    if [[ ! -f "$DB_PATH" ]]; then
        log_error "No database found at $DB_PATH"
        exit 1
    fi

    printf "\n%s=== ZFS Rebalance Status ===%s\n\n" "$C_BOLD" "$C_RESET"

    # Runs overview
    printf "%sRuns:%s\n" "$C_BOLD" "$C_RESET"
    local runs_data
    if [[ -n "$target" ]]; then
        local esc_target
        esc_target=$(sql_escape "$target")
        runs_data=$(db_query "SELECT id, pid, target, status, started_at, finished_at, files_ok, files_fail, files_skip, bytes, duration_s FROM runs WHERE target LIKE '${esc_target}%' ORDER BY id DESC LIMIT 20;")
    else
        runs_data=$(db_query "SELECT id, pid, target, status, started_at, finished_at, files_ok, files_fail, files_skip, bytes, duration_s FROM runs ORDER BY id DESC LIMIT 20;")
    fi

    if [[ -z "$runs_data" ]]; then
        printf "  No runs recorded.\n"
    else
        printf "  %-4s %-7s %-12s %-40s %-6s %-6s %-6s %-10s\n" "ID" "PID" "Status" "Target" "OK" "Fail" "Skip" "Size"
        printf "  %-4s %-7s %-12s %-40s %-6s %-6s %-6s %-10s\n" "----" "-------" "------------" "----------------------------------------" "------" "------" "------" "----------"
        while IFS='|' read -r rid rpid rtarget rstatus rstarted rfinished rok rfail rskip rbytes rduration; do
            [[ -z "$rid" ]] && continue
            local alive=""
            if [[ "$rstatus" == "running" ]]; then
                if kill -0 "$rpid" 2>/dev/null; then
                    alive=" (alive)"
                else
                    alive=" (DEAD)"
                fi
            fi
            local size_str
            size_str=$(format_bytes "${rbytes:-0}")
            printf "  %-4s %-7s %-12s %-40s %-6s %-6s %-6s %-10s\n" \
                "$rid" "$rpid" "${rstatus}${alive}" \
                "$(printf '%.40s' "$rtarget")" \
                "${rok:-0}" "${rfail:-0}" "${rskip:-0}" "$size_str"
        done <<< "$runs_data"
    fi

    # File status counts
    printf "\n%sFile status counts:%s\n" "$C_BOLD" "$C_RESET"
    local status_counts
    if [[ -n "$target" ]]; then
        local esc_target
        esc_target=$(sql_escape "$target")
        status_counts=$(db_query "SELECT status, COUNT(*) FROM files WHERE path LIKE '${esc_target}%' GROUP BY status ORDER BY status;")
    else
        status_counts=$(db_query "SELECT status, COUNT(*) FROM files GROUP BY status ORDER BY status;")
    fi

    if [[ -z "$status_counts" ]]; then
        printf "  No files tracked.\n"
    else
        while IFS='|' read -r fstatus fcount; do
            [[ -z "$fstatus" ]] && continue
            printf "  %-12s %s\n" "$fstatus" "$fcount"
        done <<< "$status_counts"
    fi

    # Total size of done files
    local total_done_bytes
    if [[ -n "$target" ]]; then
        local esc_target
        esc_target=$(sql_escape "$target")
        total_done_bytes=$(db_query "SELECT COALESCE(SUM(orig_size),0) FROM files WHERE status='done' AND path LIKE '${esc_target}%';")
    else
        total_done_bytes=$(db_query "SELECT COALESCE(SUM(orig_size),0) FROM files WHERE status='done';")
    fi
    total_done_bytes=$(( total_done_bytes + 0 ))
    printf "\n  Total rebalanced: %s\n" "$(format_bytes "$total_done_bytes")"

    printf "\n"
    exit 0
}

# ---------------------------------------------------------------------------
# Command: --verify
# ---------------------------------------------------------------------------
cmd_verify() {
    local target="$1"

    if [[ ! -f "$DB_PATH" ]]; then
        log_error "No database found at $DB_PATH"
        exit 1
    fi

    log_info "Verifying rebalanced files in: $target"

    local esc_target
    esc_target=$(sql_escape "$target")

    local total=0
    local ok=0
    local missing=0
    local size_mismatch=0

    while IFS='|' read -r fpath forig_size; do
        [[ -z "$fpath" ]] && continue
        (( total++ ))

        if [[ ! -f "$fpath" ]]; then
            log_error "MISSING: $fpath"
            (( missing++ ))
            continue
        fi

        local cur_size
        cur_size=$(stat -c %s "$fpath" 2>/dev/null) || cur_size=-1
        if [[ "$cur_size" != "$forig_size" ]]; then
            log_error "SIZE MISMATCH: $fpath (expected=$forig_size, got=$cur_size)"
            (( size_mismatch++ ))
            continue
        fi

        (( ok++ ))
    done < <(db_query "SELECT path, orig_size FROM files WHERE status='done' AND path LIKE '${esc_target}%';")

    printf "\n%sVerification results:%s\n" "$C_BOLD" "$C_RESET"
    printf "  Total:          %d\n" "$total"
    printf "  OK:             %d\n" "$ok"
    printf "  Missing:        %d\n" "$missing"
    printf "  Size mismatch:  %d\n" "$size_mismatch"
    printf "\n"

    if (( missing > 0 || size_mismatch > 0 )); then
        exit 1
    fi
    exit 0
}

# ---------------------------------------------------------------------------
# Command: --dry-run
# ---------------------------------------------------------------------------
cmd_dry_run() {
    local target="$1"

    log_info "Dry run for: $target"

    # Enumerate
    find "$target" -type f ! -name "*.balance" -printf '%i\t%s\t%p\n' > "$TMP_FILES" 2>/dev/null

    local total_files_found
    total_files_found=$(wc -l < "$TMP_FILES" 2>/dev/null)
    total_files_found=$(( total_files_found + 0 ))

    local total_bytes
    total_bytes=$(awk -F'\t' '{s+=$2} END {print s+0}' "$TMP_FILES")

    local largest_file
    largest_file=$(awk -F'\t' 'BEGIN{m=0} {if($2>m) m=$2} END {print m+0}' "$TMP_FILES")

    # Count already done
    local already_done=0
    if [[ -f "$DB_PATH" ]] && [[ "$no_db" != "true" ]]; then
        local esc_target
        esc_target=$(sql_escape "$target")
        already_done=$(db_query "SELECT COUNT(*) FROM files WHERE status='done' AND path LIKE '${esc_target}%' AND passes >= ${max_passes};")
        already_done=$(( already_done + 0 ))
    fi

    local remaining=$(( total_files_found - already_done ))
    if (( remaining < 0 )); then remaining=0; fi

    # Free space
    local free_space
    free_space=$(df --output=avail -B1 "$target" 2>/dev/null | tail -1 | tr -d ' ')

    printf "\n%s=== Dry Run Results ===%s\n\n" "$C_BOLD" "$C_RESET"
    printf "  Target:        %s\n" "$target"
    printf "  Total files:   %d\n" "$total_files_found"
    printf "  Total size:    %s\n" "$(format_bytes "$total_bytes")"
    printf "  Largest file:  %s\n" "$(format_bytes "$largest_file")"
    printf "  Already done:  %d\n" "$already_done"
    printf "  Remaining:     %d\n" "$remaining"
    if [[ -n "$free_space" ]] && (( free_space > 0 )); then
        printf "  Free space:    %s\n" "$(format_bytes "$free_space")"
        if (( largest_file > free_space )); then
            printf "  %sWARNING: Insufficient free space for largest file!%s\n" "$C_RED" "$C_RESET"
        fi
    fi

    # Estimate time (rough: 50 MB/s average throughput)
    if (( remaining > 0 && total_files_found > 0 )); then
        local avg_file_size=$(( total_bytes / total_files_found ))
        local remaining_bytes=$(( avg_file_size * remaining ))
        local est_secs=$(( remaining_bytes / 52428800 ))  # 50 MB/s
        if (( est_secs < 60 )); then est_secs=60; fi
        printf "  Est. time:     %s (at ~50 MB/s)\n" "$(format_duration "$est_secs")"
    fi

    printf "\n"

    # Clean temp files
    rm -f "$TMP_FILES" "$TMP_SORTED" "$TMP_GROUPED" "$TMP_BATCH" 2>/dev/null || true
    exit 0
}

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
print_usage() {
    cat << 'USAGE'
ZFS In-Place Rebalancing Tool v3

USAGE:
    zfs-rebalance.sh [OPTIONS] TARGET_DIRECTORY
    zfs-rebalance.sh --status [TARGET]
    zfs-rebalance.sh --verify TARGET
    zfs-rebalance.sh --dry-run TARGET

DESCRIPTION:
    Redistributes data across ZFS pool vdevs by copying each file in-place.
    After adding a new vdev, existing data stays on old vdevs. This script
    forces redistribution by: copy -> fsync -> verify -> atomic replace.

    All state tracked in SQLite database (SCRIPT_DIR/rebalance.db).
    Safe to interrupt (Ctrl+C) and resume.
    Crash recovery on next run.

OPTIONS:
    -c, --checksum true|false
        Enable/disable byte-by-byte verification with cmp (default: true).
        When false, only stat metadata is compared (faster but less safe).

    -p, --passes N
        Number of rebalancing passes (default: 1).
        Files already processed N times are skipped.

    --no-db
        Disable SQLite tracking. No crash recovery, no skip-if-done.
        Useful for one-shot runs on small directories.

    --skip-orphan-check
        Skip scanning for orphaned .balance files on startup.
        Faster startup for large directories.

    --debug
        Enable verbose debug output.

    --status [TARGET]
        Show database status: runs, file counts, active PIDs.
        Optional TARGET filters to a specific directory.

    --verify TARGET
        Verify all 'done' files: exist on disk, size matches.

    --dry-run TARGET
        Enumerate files, show stats, estimate time. No files touched.

    --version
        Show version and exit.

    -h, --help
        Show this help and exit.

EXAMPLES:
    # Rebalance a directory:
    ./zfs-rebalance.sh /mnt/pool/data/music

    # Dry run first:
    ./zfs-rebalance.sh --dry-run /mnt/pool/data/

    # Check what happened:
    ./zfs-rebalance.sh --status

    # Run with 2 passes, no checksum:
    ./zfs-rebalance.sh -p 2 -c false /mnt/pool/data/

    # Verify results:
    ./zfs-rebalance.sh --verify /mnt/pool/data/music

SAFETY:
    - Original file is NEVER deleted without a verified, fsynced copy.
    - sync after cp (forces fsync - cp does NOT fsync on its own).
    - Atomic rename (mv -f) for replacement.
    - Atomic hardlink update (ln -f uses temp+rename internally).
    - Per-file state machine in SQLite for crash recovery.
    - Safe to run from nohup, screen, tmux, systemd.

REQUIREMENTS:
    - bash 4+
    - sqlite3 3.24+
    - GNU coreutils 8.24+ (sync with file argument)
    - GNU find with -printf
USAGE
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
parse_args() {
    while (( $# > 0 )); do
        case "$1" in
            -c|--checksum)
                shift
                if (( $# == 0 )); then
                    log_error "--checksum requires an argument (true|false)"
                    exit 1
                fi
                case "$1" in
                    true|yes|1)  use_checksum=true ;;
                    false|no|0)  use_checksum=false ;;
                    *)
                        log_error "Invalid --checksum value: $1 (use true|false)"
                        exit 1
                        ;;
                esac
                shift
                ;;
            -p|--passes)
                shift
                if (( $# == 0 )); then
                    log_error "--passes requires a number"
                    exit 1
                fi
                if ! [[ "$1" =~ ^[0-9]+$ ]] || (( $1 < 1 )); then
                    log_error "Invalid --passes value: $1 (must be >= 1)"
                    exit 1
                fi
                max_passes=$1
                shift
                ;;
            --no-db)
                no_db=true
                shift
                ;;
            --skip-orphan-check)
                skip_orphan_check=true
                shift
                ;;
            --debug)
                debug_mode=true
                shift
                ;;
            --status)
                shift
                cmd_status "${1:-}"
                ;;
            --verify)
                shift
                if (( $# == 0 )); then
                    log_error "--verify requires a target directory"
                    exit 1
                fi
                cmd_verify "$1"
                ;;
            --dry-run)
                shift
                if (( $# == 0 )); then
                    log_error "--dry-run requires a target directory"
                    exit 1
                fi
                cmd_dry_run "$1"
                ;;
            --version)
                printf "zfs-rebalance %s\n" "$VERSION"
                exit 0
                ;;
            -h|--help)
                print_usage
                exit 0
                ;;
            -*)
                log_error "Unknown option: $1"
                print_usage
                exit 1
                ;;
            *)
                if [[ -n "$target_path" ]]; then
                    log_error "Multiple targets not supported. Got: $target_path and $1"
                    exit 1
                fi
                target_path="$1"
                shift
                ;;
        esac
    done
}

# ---------------------------------------------------------------------------
# Setup logging with tee
# ---------------------------------------------------------------------------
setup_logging() {
    mkdir -p "$LOG_DIR" 2>/dev/null || {
        log_error "Cannot create log directory: $LOG_DIR"
        exit 1
    }

    local timestamp
    timestamp=$(date +"%Y%m%d-%H%M%S")
    log_file_path="${LOG_DIR}/zfs-rebalance-${timestamp}-$$.log"

    # Redirect stdout through tee: colored to terminal, ANSI-stripped to log file
    exec > >(tee >(sed 's/\x1b\[[0-9;]*m//g' >> "$log_file_path"))
    exec 2>&1

    log_info "Log file: $log_file_path"
}

# ---------------------------------------------------------------------------
# Main processing loop
# ---------------------------------------------------------------------------
run_main() {
    while IFS= read -r line; do
        [[ -z "$line" ]] && continue

        if [[ "$interrupted" == "true" ]]; then
            break
        fi

        # Parse grouped line: inode TAB path1 TAB path2 TAB ...
        local -a group_paths=()
        local old_ifs="$IFS"
        IFS=$'\t'
        # shellcheck disable=SC2206
        local -a parts=($line)
        IFS="$old_ifs"

        # First element is inode, rest are paths
        local i
        for (( i = 1; i < ${#parts[@]}; i++ )); do
            [[ -n "${parts[$i]}" ]] && group_paths+=("${parts[$i]}")
        done

        if (( ${#group_paths[@]} > 0 )); then
            process_inode_group "${group_paths[@]}"
        fi

    done < "$TMP_GROUPED"

    # Final batch flush
    db_batch_flush
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    parse_args "$@"

    # Validate target
    if [[ -z "$target_path" ]]; then
        log_error "No target directory specified"
        print_usage
        exit 1
    fi

    # Normalize target path: resolve to absolute, remove trailing slash
    if [[ ! -d "$target_path" ]]; then
        log_error "Target is not a directory: $target_path"
        exit 1
    fi
    target_path="$(cd "$target_path" && pwd)"

    # Setup logging
    setup_logging

    # Print header
    printf "\n%s========================================%s\n" "$C_BOLD" "$C_RESET"
    printf "%s  ZFS Rebalance v%s%s\n" "$C_BOLD" "$VERSION" "$C_RESET"
    printf "%s========================================%s\n" "$C_BOLD" "$C_RESET"
    log_info "Target:     $target_path"
    log_info "Checksum:   $use_checksum"
    log_info "Passes:     $max_passes"
    log_info "No-DB:      $no_db"
    log_info "Debug:      $debug_mode"
    log_info "PID:        $$"
    log_info "Script dir: $SCRIPT_DIR"
    printf "\n"

    # Initialize database
    db_init

    # Register this run
    start_time=$(date +%s)
    if [[ "$no_db" != "true" ]]; then
        local esc_target
        esc_target=$(sql_escape "$target_path")
        db_exec "INSERT INTO runs (pid, target, status) VALUES ($$, '${esc_target}', 'running');"
        run_id=$(db_query "SELECT MAX(id) FROM runs;")
        run_id=$(( run_id + 0 ))
        log_info "Run ID: $run_id"
    fi

    # Recovery: handle crashed runs from previous invocations
    recover_crashed_runs

    # Check for orphaned .balance files
    check_orphaned_balance_files "$target_path"

    # Load file cache (already processed files)
    load_file_cache "$target_path"

    # Enumerate files
    if ! enumerate_files "$target_path"; then
        log_info "Nothing to process"
        exit 0
    fi

    # Save directory mtimes to SQLite for crash recovery
    save_dir_mtimes "$target_path" "$run_id"

    # Process files
    log_info "Starting rebalance processing..."
    printf "\n"

    run_main

    # cleanup() handles: final flush, run status update, dir mtime restore, summary
    # It runs via EXIT trap
}

main "$@"
