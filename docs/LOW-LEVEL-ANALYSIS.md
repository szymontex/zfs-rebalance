# Low-Level Analysis - Syscalls, Page Cache, ZFS Internals

Analysis of every command used in the rebalancing loop at the syscall level.
Each command traced from userspace through kernel to physical disk.

---

## VULNERABILITY #1: cp does NOT fsync (CRITICAL)

### The problem

GNU cp (coreutils) calls this sequence:
```
open(src, O_RDONLY)
open(dst, O_WRONLY|O_CREAT|O_TRUNC)
[loop] read(src) -> write(dst)       # 256KB blocks
utimensat(dst, timestamps)
fchown(dst, uid, gid)
fchmod(dst, mode)
close(dst)
close(src)
# NO fsync(). NO fdatasync(). NEVER.
```

**Confirmed from coreutils source code**: cp NEVER calls fsync or fdatasync.
The `-a` flag does not change this. There is no `--fsync` flag.

After `cp -a original .balance` returns exit code 0:
- .balance data is in the kernel **page cache** (RAM)
- .balance data is **NOT guaranteed to be on disk**
- On ZFS: data waits in transaction buffers for the next TXG commit (~5 seconds)

### Why this matters

Our pipeline was: cp → cmp → mv. Consider:

```
1. cp writes .balance        (data in page cache, not on disk)
2. cmp reads original + .balance  (reads from page cache - sees correct data)
3. cmp says: MATCH            (correct! but comparing RAM, not disk)
4. mv -f .balance original    (atomic rename in kernel)
5. ... 3 seconds pass ...
6. POWER LOSS                  (TXG with .balance data hasn't committed)
7. Reboot: .balance data LOST  (uncommitted TXG rolled back)
8. "original" now has 0 bytes or partial content
```

**Result: DATA LOSS.** The original file is replaced by an empty or partial copy.

cmp told us the copy was good. It was - in RAM. But it wasn't on disk yet.

### The fix

```bash
cp --reflink=never -ax "$original" "$copy"
sync "$copy"                              # <-- forces fsync(), TXG commit on ZFS
cmp -s "$original" "$copy"                # now comparing against on-disk data
mv -f "$copy" "$original"
```

`sync FILE` (coreutils 8.24+) calls fsync() on the specific file. On ZFS,
fsync() forces the current TXG to commit - data is written to disk before
sync returns. Ubuntu 22.04 has coreutils 8.32 - supported.

### Cost

fsync on ZFS forces a TXG commit. Cost:
- Small file (~100KB): ~5-50ms (TXG commit overhead)
- Large file (~1GB): ~50-200ms (already spans multiple TXGs)
- Very large file (~50GB): negligible (data already committed via previous TXGs)

For 790k files at ~5ms average: ~66 minutes total fsync overhead across the
entire run. On a 2-day operation, this is 2% overhead. Acceptable.

### Why sync before cmp, not after cmp?

If we sync after cmp: cmp compared page-cache data (might not match what's on
disk if partial TXG commit). Better to sync first, ensuring what's on disk is
complete, THEN verify it.

Actually: cmp will read from page cache regardless (kernel returns cached data).
The sync ensures the page cache content IS on disk. So sync-before-cmp and
sync-after-cmp both work for the "verify what's on disk" purpose. But
sync-before-cmp is conceptually cleaner: "flush to disk, then verify."

---

## VULNERABILITY #2: Hardlink relink window (MODERATE)

### The problem (as described in v2.1)

```bash
rm -f "$link_path"              # old link gone from disk
ln "$main_file" "$link_path"    # new link created
```

Between rm and ln: path doesn't exist. If crash here, the path is permanently
missing (until recovery recreates it from DB).

### The fix: ln via temp + atomic rename

GNU coreutils `ln -f` actually does this internally:
```
linkat(src, tmpname)            # create temp hardlink "CuXXXXXX"
renameat(tmpname, destination)  # atomic replace destination with temp
```

So `ln -f main_file link_path` ALREADY does atomic replacement. The path
always exists (either old inode or new inode, never absent).

But we're currently doing `rm + ln` (two operations). Change to:

```bash
ln -f "$main_file" "$link_path"   # atomic: creates temp, renames over old
```

One command instead of two. No window where the path doesn't exist.

If crash during ln -f:
- After linkat but before renameat: temp file CuXXXXXX exists, old link intact
- After renameat: new link is in place
- In either case: link_path always has a valid file behind it

The only orphan is the CuXXXXXX temp (easily cleaned up by finding files matching
that pattern, though this is extremely unlikely in practice).

---

## VULNERABILITY #3: fsync on rename (LOW on ZFS)

### The problem

rename() is atomic **in kernel memory** - other processes always see either
old or new state. But on many filesystems (ext4 writeback mode), rename is
NOT atomic to power failure without fsync on the directory.

### On ZFS: not a vulnerability

ZFS rename is part of a ZFS transaction (TXG). Both the source unlink and
target link are in the same TXG. After TXG commit: rename is fully durable.
Before TXG commit: rename didn't happen (rolled back to previous state).

ZFS guarantees: after power recovery, rename either fully happened or fully
didn't. No partial state.

The only risk: rename happened in kernel but TXG didn't commit before power loss.
Then rename "unhappens" on reboot. This means:
- .balance still exists (old name)
- original still has old inode
- Status in DB: 'replacing'
- Recovery: detect this case (current_inode == orig_inode, .balance exists),
  re-verify and re-mv. No data loss.

For extra safety: sync the directory after rename.
```bash
mv -f "$copy" "$original"
sync "$(dirname "$original")"    # ensure directory entry change is on disk
```

This forces the TXG with the rename to commit. Cost: one fsync per file.

---

## VULNERABILITY #4: cp creates empty file before writing (LOW)

### The problem

cp opens the destination with O_CREAT|O_TRUNC. This creates the .balance file
(or truncates it to 0) BEFORE any data is written. If SIGKILL arrives after
open but before any write:

- .balance exists with 0 bytes
- Status: 'copying' (we set status before cp)

### Recovery

Status='copying' + .balance exists with size 0 → clearly incomplete.
0 != orig_size → delete .balance, reset to 'pending'.

Our existing recovery handles this correctly. Not a new vulnerability, just
confirming the edge case exists and is covered.

---

## VULNERABILITY #5: cmp reads from page cache, not disk (INFORMATIONAL)

### The situation

cmp uses read() syscall. Linux kernel returns data from the page cache if
available. After cp just wrote .balance, the data is hot in page cache.
cmp reads it from there - not from disk.

### Why this is NOT a vulnerability (with the sync fix)

After `sync "$copy"`:
1. fsync() forces data to disk (TXG commit on ZFS)
2. Page cache still has the data
3. cmp reads from page cache - sees the same data that's now also on disk
4. Comparison is valid

Without sync: cmp sees correct data (from cache) but that data might not be
on disk. This is vulnerability #1, already fixed by adding sync.

### Can page cache have stale data?

No, under normal Linux operation. The page cache is the single source of truth
for file data. write() updates the page cache, read() returns from page cache.
They're always consistent within a single system (no NFS/cluster concerns).

---

## VULNERABILITY #6: xattr and ACL not verified (LOW)

### The problem

cp -a copies xattrs and ACLs. Our verification only checks:
- stat: permissions, owner, group, size, mtime
- cmp: byte-by-byte content

We do NOT check:
- Extended attributes (xattr)
- POSIX ACLs
- ZFS ACLs

If cp fails to copy an xattr or ACL, we won't catch it.

### Assessment

For audio studio files: xattrs are rarely used. Samba stores DOS attributes
in xattrs (user.DOSATTRIB, user.DosStream.*) but these are auto-generated
by Samba on access, not stored by users.

### Mitigation options

A) Accept the gap (low risk, simpler code)
B) Add xattr comparison:
```bash
getfattr -d "$original" > /tmp/xattr_orig
getfattr -d "$copy" > /tmp/xattr_copy
diff -q /tmp/xattr_orig /tmp/xattr_copy
```

Recommendation: accept the gap for now. Document as known limitation.

---

## VULNERABILITY #7: SQLite DB on same filesystem as data (MODERATE)

### The problem

If the SQLite DB is on the ZFS pool being rebalanced:
- Heavy I/O from rebalancing triggers ZFS write throttle
- SQLite writes compete with cp writes for TXG space
- busy_timeout could be exceeded if write throttle delays SQLite

### Assessment for this deployment

The script lives in ~/scripts/ which is on /dev/mapper/ubuntu--vg-ubuntu--lv
(ext4, root filesystem). The ZFS pool is on separate disks.

**DB is on ext4, data is on ZFS. No cross-interference.** This is the ideal
setup. No action needed.

### General recommendation

Always place the script (and thus the SQLite DB) on a different filesystem
than the ZFS pool being rebalanced. This avoids write throttle interference
and ensures SQLite operations are fast regardless of ZFS I/O load.

---

## VULNERABILITY #8: Batch flush interval and crash window (LOW)

### The problem

We flush file status to SQLite every 1000 files. If crash occurs between
flushes, up to 999 successfully processed files are not recorded in the DB.

On next run, those files are treated as unprocessed and rebalanced again.
No data loss - just wasted I/O. But for large files, this could mean hours
of redundant work.

### Mitigation

Reduce batch interval to 100 files. Cost: 10x more SQLite writes.
- 790k files / 100 = 7,900 writes
- Each write: ~10ms
- Total: ~79 seconds of SQLite overhead
- Acceptable

Alternatively: immediate flush for files larger than a threshold (e.g., 1GB).
Large files are the ones where re-processing is most expensive.

```bash
# After processing each file:
if (( file_size > 1073741824 )); then
    flush_batch_now    # don't wait for batch interval
fi
```

---

## VULNERABILITY #9: TXG timing for directory mtime restore (LOW)

### The problem

On exit, we restore directory mtimes with `touch -m -d "@$timestamp" "$dir"`.
These touch operations go through write() → page cache → TXG.

If power loss after touch but before TXG commit: directory mtimes are not
restored. They show today's date instead of the original.

### Assessment

This is cosmetic (SMB users see wrong folder dates) not data-critical.
On clean shutdown or Ctrl+C: the trap runs, touch happens, and normal TXG
flush within 5 seconds persists it.

On kill -9: dir_mtimes are in SQLite, restored on next run.

On power loss: dir_mtimes might not be restored in this run, but are in
SQLite for recovery. Acceptable.

---

## Final safe pipeline (incorporating all fixes)

```bash
# 0. Record intent
sqlite3 "$DB" "INSERT ... status='pending'"

# 1. Copy
sqlite3 "$DB" "UPDATE ... status='copying'"
cp --reflink=never -ax "$original" "$copy"

# 2. Sync copy to disk (FIX for vulnerability #1)
sync "$copy"

# 3. Verify (now comparing against on-disk data)
sqlite3 "$DB" "UPDATE ... status='verifying'"
cmp -s "$original" "$copy"

# 4. Atomic replace
sqlite3 "$DB" "UPDATE ... status='replacing'"
mv -f "$copy" "$original"

# 5. Sync directory entry (FIX for vulnerability #3)
sync "$(dirname "$original")"

# 6. Hardlinks (FIX for vulnerability #2)
sqlite3 "$DB" "UPDATE ... status='relinking'"
for link in "${other_links[@]}"; do
    ln -f "$main_file" "$link"    # atomic via temp+rename internally
done

# 7. Record completion
sqlite3 "$DB" "UPDATE ... status='done', new_inode=..."
```

### Crash at any point in this pipeline:

| Crash point | Disk state | Recovery |
|---|---|---|
| Before/during cp | original intact, .balance partial/missing | Delete .balance, retry |
| After cp, before sync | original intact, .balance in page cache | If power loss: .balance lost. Recovery: re-copy |
| After sync, before cmp | original intact, .balance on disk | Re-verify |
| During cmp | original intact, .balance on disk (read-only) | Re-verify |
| After cmp, before mv | original intact, .balance verified on disk | Retry mv |
| During mv (kernel) | ZFS: atomic in TXG. Either old or new state | Check inode, handle accordingly |
| After mv, before dir sync | original replaced. Dir might unrename on power loss | On ZFS: TXG atomicity means unlikely. Dir sync is extra safety |
| During relinking | main_file safe. ln -f is atomic per path | Check each path, relink missing ones |
| After relinking, before DB update | All files correct on disk | Update DB status |

**Zero data loss at every point.** Either the original file exists, or a
verified-and-fsynced copy exists, or both exist. Never neither.
