# zfs-rebalance

Production-grade ZFS in-place rebalancing tool. Redistributes data across vdevs after adding new mirrors to a pool.

Inspired by [zfs-inplace-rebalancing](https://github.com/markusressel/zfs-inplace-rebalancing). Rewritten from scratch with SQLite tracking, fsync barriers, atomic operations, and crash recovery for production use on large pools (60TB+).

## Why not the original?

The upstream script works for small pools, but has issues that matter at scale:

- `set -e` kills the script on any error (including expected ones)
- `rm` + `mv` is not atomic - crash between them = data at risk
- `cp` does not call `fsync` - power loss after "verified" copy = data loss
- `rm` + `ln` for hardlinks has a window where the path doesn't exist
- Flat-file DB with `grep` = O(n) per lookup, hours of overhead on 800k files
- No crash recovery, no run history, no concurrent access support

## How it works

For each file in the target directory:

1. **Record** - save original metadata (size, mtime, inode) to SQLite
2. **Copy** - `cp --reflink=never -ax` (force real data copy, not block clone)
3. **Sync** - `sync` (fsync barrier - forces data to physical disk)
4. **Verify** - `stat` comparison + `cmp -s` (byte-by-byte content check)
5. **Replace** - `mv -f` (atomic `rename()` syscall on same filesystem)
6. **Relink** - `ln -f` for hardlink groups (atomic per-path via temp+rename)

ZFS allocates new writes proportionally to free space across vdevs, so the fresh copy lands on the least-full vdevs.

## Safety guarantees

- **Atomic replace** via `rename()` - file always exists (old or new, never gone)
- **fsync after copy** - GNU `cp` never calls fsync. Without explicit sync, "verified" data might only be in RAM. Power loss = data loss. We sync before verification.
- **fsync after rename** - forces directory entry change to disk (ZFS TXG commit)
- **Per-file status tracking** in SQLite - 8 states (pending, copying, syncing, verifying, replacing, relinking, done, failed). On crash, status tells exactly where we were and what recovery action is safe.
- **Original metadata in DB** - size, mtime, inode recorded BEFORE any operation. Every recovery decision compares current disk state against these values, never assumptions.
- **No `set -e`** - errors are caught and handled, never crash the script
- **No `rm -rf` anywhere** - only `rm -f` on specific `.balance` files
- **Crash recovery on restart** - detects crashed runs (dead PID), recovers per-file based on status, restores directory mtimes from DB
- **Orphan `.balance` detection** - finds leftover copies from crashed runs
- **Directory mtime preservation** - Windows/SMB clients see original folder dates
- **Concurrent access** - SQLite WAL mode, tested with 30 simultaneous instances

## What can go wrong (and how we handle it)

See [FAILURE-ANALYSIS.md](docs/FAILURE-ANALYSIS.md) for exhaustive analysis of every instruction in the processing loop - what happens on crash/error at each point, what the disk and DB state is, and what recovery action is taken.

See [LOW-LEVEL-ANALYSIS.md](docs/LOW-LEVEL-ANALYSIS.md) for syscall-level analysis of `cp`, `mv`, `cmp`, `sync`, `ln`, and their interaction with ZFS page cache and TXG commits.

## Requirements

- Linux with ZFS (tested on ZFS 2.1.5, Ubuntu 22.04)
- bash 4+ (associative arrays)
- sqlite3 3.24+ (pre-installed on Ubuntu 22.04)
- GNU coreutils 8.24+ (`sync` with file argument)
- GNU `find` with `-printf`
- For ZFS 2.3.3+: consider native `zfs rewrite` command instead

## Installation

```bash
# Clone
git clone https://github.com/YOUR_USERNAME/zfs-rebalance.git
cd zfs-rebalance

# That's it. Single script, no build step, no dependencies beyond sqlite3.
chmod +x zfs-rebalance.sh
```

## Usage

```bash
# Rebalance a directory (run as root for full attribute preservation):
sudo ./zfs-rebalance.sh /mnt/pool/data

# Dry run - show stats without touching files:
sudo ./zfs-rebalance.sh --dry-run /mnt/pool/data

# Check status of all runs:
./zfs-rebalance.sh --status

# Verify previously rebalanced files still exist with correct sizes:
sudo ./zfs-rebalance.sh --verify /mnt/pool/data

# Multiple passes:
sudo ./zfs-rebalance.sh -p 2 /mnt/pool/data

# Skip byte-level verification (faster, stat-only):
sudo ./zfs-rebalance.sh -c false /mnt/pool/data
```

### Long-running jobs

Always use `screen` or `tmux` for large directories:

```bash
screen -S rebalance
sudo ./zfs-rebalance.sh /mnt/pool/data
# Ctrl+A D to detach. screen -r rebalance to reattach.
```

### Parallel execution

Safe to run multiple instances on different directories simultaneously. SQLite WAL mode handles concurrent writes natively.

```bash
screen -S rebal-projects
sudo ./zfs-rebalance.sh /mnt/pool/projects

# In another screen:
screen -S rebal-samples
sudo ./zfs-rebalance.sh /mnt/pool/samples
```

### After a crash

Just re-run the same command. The script will:

1. Detect the crashed run (dead PID in DB)
2. Recover each incomplete file based on its status
3. Restore directory mtimes from DB
4. Skip already-completed files
5. Continue processing remaining files

### Monitoring

```bash
# Follow progress in real-time:
tail -f logs/zfs-rebalance-*.log

# Quick status:
./zfs-rebalance.sh --status

# Check for errors:
grep "ERR" logs/zfs-rebalance-*.log

# Watch vdev balance:
watch zpool list -v
```

## Options

| Flag | Default | Description |
|------|---------|-------------|
| `-c`, `--checksum` | `true` | Byte-level verification with `cmp` before replacing |
| `-p`, `--passes` | `1` | Max rebalance passes per file |
| `--no-db` | off | Skip SQLite tracking (no resume, no history) |
| `--skip-orphan-check` | off | Don't scan for orphaned `.balance` files on start |
| `--debug` | off | Verbose output |
| `--status [TARGET]` | - | Show DB status, run history, file counts |
| `--verify TARGET` | - | Verify rebalanced files exist with correct sizes |
| `--dry-run TARGET` | - | Show stats without touching files |

## How state is stored

All state lives next to the script - no CWD dependency, no files in your data directories:

```
zfs-rebalance/
  zfs-rebalance.sh          # the script
  rebalance.db              # SQLite database (auto-created)
  logs/                     # log files (auto-created)
```

The SQLite database tracks:
- **files** - per-file status, original metadata, pass count
- **runs** - run history with PID, target, stats, timestamps
- **dir_mtimes** - directory modification times for crash recovery

## Logging

Output goes to both terminal (colored) and log file (plain text). Log files are grep-friendly:

```bash
grep "ERR"           logs/*.log    # errors only
grep "FAIL:"         logs/*.log    # failed files with reason
grep "MANUAL ACTION" logs/*.log    # files needing manual intervention
grep "^\["           logs/*.log    # progress lines with file sizes
```

## Performance

Tested on 87T ZFS pool (6x mirror), 62T data:

| Metric | Value |
|--------|-------|
| Small files (<1MB) | ~400-1000 files/min |
| Large files (>100MB) | I/O bound (~200 MB/s) |
| fsync overhead | ~2% of total runtime |
| SQLite overhead | negligible (batch writes every 100 files) |
| Memory usage | ~240MB for 800k file cache |

## License

MIT

## Acknowledgments

Inspired by [markusressel/zfs-inplace-rebalancing](https://github.com/markusressel/zfs-inplace-rebalancing).
