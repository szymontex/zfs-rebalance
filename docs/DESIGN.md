# ZFS In-Place Rebalancing v3 - Design Spec

## Overview

Complete rewrite of the ZFS rebalancing script. Replaces flat-file DB with SQLite
for concurrent access, crash recovery, and queryable run history. Self-contained
directory: script + state + logs in one place.

Production target: 87T ZFS pool, 6x mirror + SLOG, 790k files, 62T data,
Ubuntu 22.04, SQLite 3.37.2, bash 5.1, ZFS 2.1.5.

## Directory Layout

```
~/scripts/zfs-rebalance/
  zfs-rebalance.sh              # the script (single file, no deps beyond sqlite3)
  rebalance.db                  # SQLite database (WAL mode)
  rebalance.db-wal              # WAL file (auto-managed by SQLite)
  rebalance.db-shm              # shared memory (auto-managed by SQLite)
  logs/
    zfs-rebalance-YYYYMMDD-HHMMSS-PID.log
```

No state in CWD. No state in data directories. CWD is irrelevant.
All paths derived from `SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)`.

**Critical: script and DB must be on a DIFFERENT filesystem than the ZFS pool
being rebalanced** (avoids ZFS write throttle interfering with SQLite writes).
On this server: script on ext4 root, data on ZFS pool. Ideal.

## SQLite Schema

```sql
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 60000;
PRAGMA synchronous = NORMAL;

-- Per-file tracking with full lifecycle status and original metadata
CREATE TABLE IF NOT EXISTS files (
    path        TEXT PRIMARY KEY,
    orig_size   INTEGER NOT NULL,          -- size BEFORE rebalancing (ground truth)
    orig_mtime  REAL NOT NULL,             -- mtime BEFORE rebalancing
    orig_inode  INTEGER NOT NULL,          -- inode BEFORE rebalancing
    new_inode   INTEGER,                   -- inode AFTER rebalancing (NULL = not done)
    passes      INTEGER NOT NULL DEFAULT 0,
    status      TEXT NOT NULL DEFAULT 'pending'
                CHECK(status IN ('pending','copying','syncing','verifying',
                                 'replacing','relinking','done','failed')),
    link_main   TEXT,              -- NULL=standalone, main path=hardlink group leader
    run_id      INTEGER,           -- which run is/was processing this file
    processed_at TEXT
) WITHOUT ROWID;

-- Active and historical runs
CREATE TABLE IF NOT EXISTS runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    pid         INTEGER NOT NULL,
    target      TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'running'
                CHECK(status IN ('running','completed','interrupted','crashed')),
    started_at  TEXT NOT NULL DEFAULT (datetime('now')),
    finished_at TEXT,
    last_file_at TEXT,             -- heartbeat: updated every batch flush
    files_ok    INTEGER NOT NULL DEFAULT 0,
    files_fail  INTEGER NOT NULL DEFAULT 0,
    files_skip  INTEGER NOT NULL DEFAULT 0,
    bytes       INTEGER NOT NULL DEFAULT 0,
    duration_s  INTEGER
);

-- Directory mtimes for crash recovery
CREATE TABLE IF NOT EXISTS dir_mtimes (
    run_id      INTEGER NOT NULL,
    path        TEXT NOT NULL,
    mtime       REAL NOT NULL,
    PRIMARY KEY (run_id, path)
) WITHOUT ROWID;
```

### Schema rationale

**files table:**
- `orig_size, orig_mtime, orig_inode` - recorded BEFORE any operation. This is
  the ground truth for crash recovery. Every recovery decision compares current
  disk state against these values. Without them, recovery is guessing.
- `new_inode` - recorded AFTER successful replace. Confirms rebalancing happened
  (inode changed = new data blocks on different vdevs).
- `status` - per-file state machine with 8 states. Each status transition is a
  gate. On crash, status tells us EXACTLY where we were and what recovery is safe.
  The `syncing` state (new vs v2) marks the window between cp completing and
  fsync completing - data in page cache but not yet on disk.
- `link_main` - for hardlink groups: all members reference the group leader.
  Critical for crash recovery: after kill -9 during relinking, the DB is the
  ONLY record of which paths should exist (find can't find deleted paths).
- WITHOUT ROWID: PK is TEXT (path). 48% smaller DB, 24% faster lookups.

**runs table:**
- `last_file_at` - heartbeat updated every batch flush. Distinguishes
  "crashed 5 min ago" from "hung for 6 hours with alive PID".
- Replaces lockfile mechanism entirely.

**dir_mtimes:**
- Keyed by run_id so concurrent runs don't clash.
- Survives kill -9 (persisted in SQLite, not just in memory).

## File Status State Machine

```
                          ┌──────────────────────────────────────────────┐
                          │         (hardlinks only)                     │
                          ▼                                              │
pending → copying → syncing → verifying → replacing → done              │
   │         │         │          │            │                          │
   │         │         │          │            └──→ relinking ──→ done   │
   │         │         │          │                     │                 │
   ▼         ▼         ▼          ▼                     ▼                 │
 failed    failed    failed     failed                failed              │
```

State transitions and what they mean:

| Status | What happened | What's on disk |
|---|---|---|
| pending | Intent recorded, nothing touched | Original intact, no .balance |
| copying | cp started | Original intact, .balance partial or complete |
| syncing | cp finished, fsync in progress | Original intact, .balance complete in page cache, possibly not on disk |
| verifying | fsync done, cmp in progress | Original intact, .balance complete AND on disk |
| replacing | cmp passed, mv -f in progress | rename() atomic: either old or new state |
| relinking | mv done, hardlinks being updated | Main file safe (new inode), some links may be old/missing |
| done | Everything complete | File rebalanced, all links correct |
| failed | Error at any stage | Original intact (or .balance preserved with ALARM) |

## Core Processing Pipeline

Per-file pipeline with fsync barriers and atomic operations:

```bash
# 0. RECORD intent (before touching anything on disk)
#    DB: INSERT with orig_size, orig_mtime, orig_inode, status='pending'
orig_size=$(stat -c %s "$file")
orig_mtime=$(stat -c %Y "$file")
orig_inode=$(stat -c %i "$file")
db_insert "$file" "$orig_size" "$orig_mtime" "$orig_inode" "pending"

# 1. COPY
#    DB: UPDATE status='copying'
#    cp does NOT call fsync. Data goes to page cache only.
#    After cp returns 0: data is in RAM, NOT guaranteed on disk.
db_update_status "$file" "copying"
cp --reflink=never -ax "$file" "$file.balance"

# 2. SYNC (fsync barrier - forces data to physical disk)
#    DB: UPDATE status='syncing'
#    sync calls fsync() on the file. On ZFS: forces TXG commit.
#    After sync returns: .balance data IS on physical disk.
#    Cost: ~5-50ms per file (SLOG accelerates this on our server).
db_update_status "$file" "syncing"
sync "$file.balance"

# 3. VERIFY (read-only comparison against on-disk data)
#    DB: UPDATE status='verifying'
#    stat: compare permissions, owner, group, size, mtime
#    cmp: byte-by-byte content comparison
#    Both read from page cache - but sync already ensured cache = disk.
#    If mismatch: rm .balance, status='failed'. Original untouched.
db_update_status "$file" "verifying"
verify_copy "$file" "$file.balance"

# 4. REPLACE (atomic rename - single syscall)
#    DB: UPDATE status='replacing'
#    mv -f uses rename() syscall. On same filesystem: atomic.
#    On ZFS: both old-name-removal and new-name-creation in same TXG.
#    At no point does the filename not have a valid inode behind it.
db_update_status "$file" "replacing"
mv -f "$file.balance" "$file"
sync "$(dirname "$file")"    # force directory entry to disk

# 5. RELINK hardlinks (only for hardlink groups)
#    DB: UPDATE status='relinking' for all group members
#    ln -f internally does: linkat(temp) + renameat(temp, dest)
#    This is atomic per-path: dest always exists (old or new inode).
#    No window where any path doesn't exist.
db_update_status_group "$file" "relinking"
for link in "${other_links[@]}"; do
    ln -f "$file" "$link"
done

# 6. DONE
#    DB: UPDATE status='done', new_inode, passes++
new_inode=$(stat -c %i "$file")
db_mark_done "$file" "$new_inode"
```

### Why each step matters

- **sync after cp**: cp NEVER calls fsync (confirmed from coreutils source).
  Without sync, power loss after cmp-says-OK means .balance data is lost.
  This was the critical vulnerability found in low-level analysis.

- **sync after mv**: rename() on ZFS is in a TXG. Without sync, power loss
  could roll back the rename (original reverts to old inode). Not data loss
  (old data is intact) but correctness loss (DB says 'done', disk says no).
  Syncing the directory forces the TXG to commit.

- **ln -f instead of rm + ln**: GNU coreutils ln -f uses temp+rename internally
  (linkat + renameat). No window where the link path doesn't exist. The old
  approach (rm + ln) had a crash window where the path was absent.

## Concurrent Access Model

SQLite WAL mode: unlimited readers, one writer at a time. Writers queue with
busy_timeout=60000 (60s). Tested with 30 concurrent writers doing batch INSERTs
- zero failures, 18000/18000 rows, 6.3 seconds total.

Each instance:
1. **Start**: SELECT files WHERE status='done' for the target path into
   bash associative array (one query)
2. **Processing**: lookups from in-memory array (O(1), zero I/O per file)
3. **Every 100 files** (or immediately for files >1GB): batch write via
   piped SQL to sqlite3 (~10ms per batch)
4. **End**: final batch write + UPDATE runs status

Batch interval is 100 files (not 1000). On crash, at most 99 successfully
processed files lose their DB records. Those files get re-processed on next
run (redundant I/O but no data loss). For files >1GB, immediate flush avoids
re-copying multi-GB files unnecessarily.

Batch writes use INSERT OR REPLACE with escaped paths (sed s/'/''/g) piped
through stdin to sqlite3.

## Temp Files

Ephemeral enumeration files in /tmp (PID-unique, cleaned on EXIT):
- `/tmp/zfs-rebalance-PID-files.txt`     - find output
- `/tmp/zfs-rebalance-PID-sorted.txt`    - sorted by inode
- `/tmp/zfs-rebalance-PID-grouped.txt`   - grouped by inode
- `/tmp/zfs-rebalance-PID-batch.sql`     - batch SQL buffer

These are 80-100MB for 790k files. Cleaned by trap on EXIT/INT/TERM/HUP.

dir_mtimes go to SQLite (persistent across crashes, not just in-memory).

## Run Lifecycle

```
START:
  1. Resolve SCRIPT_DIR, DB_PATH, LOG path
  2. Init SQLite (create tables if not exist, set PRAGMAs)
  3. INSERT INTO runs (pid, target, status='running') -> get run_id
  4. Recover crashed runs:
     a. SELECT runs WHERE status='running' AND pid is dead
     b. For each: mark 'crashed', restore dir_mtimes
     c. Recover incomplete files (per-status logic, see FAILURE-ANALYSIS.md)
     d. Scan for untracked orphan .balance files
  5. Load processed files (status='done') into associative array

ENUMERATE:
  6. find -printf to temp file (fast, no subprocess per file)
  7. Pre-run stats (file count, total size, largest file)
  8. Free space check (largest file must fit)
  9. Sort + group by inode (hardlink detection)
  10. Save dir mtimes to SQLite

PROCESS (per file):
  11. Record: INSERT with orig metadata, status='pending'
  12. Copy: UPDATE status='copying', cp --reflink=never -ax
  13. Sync: UPDATE status='syncing', sync (fsync to disk)
  14. Verify: UPDATE status='verifying', stat + cmp
  15. Replace: UPDATE status='replacing', mv -f, sync dir
  16. (Hardlinks): UPDATE status='relinking', ln -f for each link
  17. Done: UPDATE status='done', new_inode, passes++
  18. Every 100 files (or >1GB): batch flush to DB, update runs.last_file_at

FINISH:
  19. Final batch flush
  20. Post-run file count check (find | wc vs pre-run count)
  21. UPDATE runs SET status='completed', stats, duration
  22. DELETE dir_mtimes for this run_id
  23. Restore dir mtimes (touch -m)
  24. Summary with failed files list, throughput, duration

INTERRUPT (Ctrl+C / SIGTERM / SIGHUP):
  19. Best-effort batch flush
  20. UPDATE runs SET status='interrupted'
  21. Restore dir mtimes
  22. Summary via log_direct (bypass potentially dead tee)
```

## Recovery on Startup

On every run, before processing, the script:

1. Finds crashed runs (status='running', dead PID via kill -0)
2. For each crashed run:
   a. Marks as status='crashed'
   b. Restores dir_mtimes for that run
   c. Scans files with status NOT IN ('done','failed') for that run_id
   d. Per-status recovery (see FAILURE-ANALYSIS.md):
      - pending: verify original matches orig_*, delete DB record, re-process
      - copying/syncing: delete .balance (NEVER trust unverified copy), reset
      - verifying: re-verify if both files exist, else handle per case
      - replacing: check inode to determine if rename() completed
      - relinking: check each path, recreate missing links via ln -f
3. Scans for orphaned .balance files not tracked in DB

**Recovery invariant**: no file is ever deleted or renamed without first
verifying it against orig_size/orig_mtime/orig_inode recorded in the DB.

## Signal Handling

```
EXIT  -> cleanup() always runs (restore mtimes, flush DB, clean temp files)
INT   -> interrupted=true; exit 130  (Ctrl+C)
TERM  -> interrupted=true; exit 143  (kill)
HUP   -> respected by nohup (bash preserves SIG_IGN); else clean exit
KILL  -> trap doesn't fire. Recovery on next run:
         - runs table: status='running' + dead PID -> mark 'crashed'
         - files table: per-status recovery with verification
         - dir_mtimes: restored from SQLite
         - .balance orphans: detected and handled based on DB status
```

## Logging

- Terminal: colored ANSI output
- Log file: plain text (ANSI stripped by sed in tee pipeline)
- `log_direct()`: writes to log file directly, bypassing tee.
  Used in cleanup when terminal may be dead (SSH disconnect kills tee via SIGPIPE).

Log location: `SCRIPT_DIR/logs/zfs-rebalance-YYYYMMDD-HHMMSS-PID.log`

Log format:
```
[INFO]  Informational
[ OK ]  Success
[WARN]  Warning
[ERR ]  Error
[DBG ]  Debug (--debug only)
[N/M] (P%) [SIZE] /path   ETA: Xh Ym
```

Useful greps:
```bash
grep "ERR"           logs/*.log    # errors only
grep "FAIL:"         logs/*.log    # failed files with reason
grep "MANUAL ACTION" logs/*.log    # files needing manual intervention
grep "^\["           logs/*.log    # progress lines with file sizes
```

## Additional Commands

```
--status [TARGET]     Show DB status, active/historical runs, file counts
--verify TARGET       Verify 'done' files: exist on disk, size matches orig_size
--dry-run TARGET      Enumerate, estimate time/space, don't touch files
--analyze             Show vdev balance, directory sizes, recommendations
--version             Show version
--help                Show help
```

## Known Limitations

1. **xattr and ACL not verified** - cp -a copies them, but our verify only
   checks stat (perms, owner, size, mtime) + cmp (content). xattrs/ACLs are
   not compared. Low risk for audio studio files; Samba auto-generates DOS
   attributes on access.

2. **Same file by two instances** - if two parallel instances both enumerate
   the same file before either processes it, both will try to rebalance it.
   The second mv -f replaces the first's result with an equally valid copy.
   No data loss, just wasted I/O. The batch flush interval (100 files) limits
   this window.

3. **File modified during cp** - if a DAW writes to a file while cp reads it,
   the copy may have mixed old/new data. cmp catches this (original changed
   between cp's read and cmp's read) and marks the file as 'failed'. No data
   loss. Don't rebalance actively written files.

4. **ZFS 2.3.3+ has native `zfs rewrite`** - if you upgrade ZFS, consider
   using the native command instead of this script. It avoids all the
   copy/verify/rename complexity.

## Dependencies

- bash 4+ (associative arrays, coproc)
- sqlite3 3.24+ (pre-installed on Ubuntu 22.04, need UPSERT support)
- GNU coreutils 8.24+ (sync with file argument)
- GNU find with -printf
- No new dependencies vs v2 (sqlite3 was already installed)
