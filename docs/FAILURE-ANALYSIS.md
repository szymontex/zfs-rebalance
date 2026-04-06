# Failure Mode Analysis - ZFS Rebalance v3

Every instruction in the file processing loop analyzed for crash/failure at
every point. For each: what is the disk state, what is the DB state, and
what is the recovery action.

Notation:
- DISK[original] = state of the original file on disk
- DISK[.balance] = state of the .balance copy on disk
- DB[status] = status field in SQLite files table
- DB[orig_*] = original file metadata recorded in DB before any operation
- CRASH = unrecoverable interruption (kill -9, power loss, kernel panic)
- ERROR = recoverable error (command returns nonzero, I/O error)
- PAGE CACHE = data in kernel memory, NOT yet on physical disk
- ON DISK = data committed to physical storage (after fsync / TXG commit)

---

## PHASE 0: Pre-processing checks

### 0.1 Check file exists

```
if [[ ! -f "$main_file" ]]; then skip; fi
```

CRASH here:
- DISK: untouched
- DB: no record for this file yet
- Recovery: nothing to recover. File processed on next run.

### 0.2 Check .balance leftover

```
if [[ -f "$tmp_file" ]]; then
    # Leftover from previous crash - handled by startup recovery
fi
```

If .balance exists from a previous crash, recovery is handled by the startup
recovery routine (PHASE R below), not here.

---

## PHASE 1: Record intent in DB

```bash
orig_size=$(stat -c %s "$main_file")
orig_mtime=$(stat -c %Y "$main_file")
orig_inode=$(stat -c %i "$main_file")

sqlite3 "$DB" "INSERT OR IGNORE INTO files
    (path, orig_size, orig_mtime, orig_inode, status, run_id)
    VALUES ('$escaped_path', $orig_size, $orig_mtime, $orig_inode, 'pending', $run_id)"
```

### 1.1 stat fails

- Cause: file vanished between -f check and stat (deleted by another process)
- DISK[original]: gone
- DB: no record
- Action: log warning, skip. No recovery needed.

### 1.2 CRASH after stat, before INSERT

- DISK[original]: untouched
- DB: no record
- Recovery: file not in DB = processed normally on next run. Clean.

### 1.3 INSERT fails (SQLite busy/full)

- DISK[original]: untouched
- DB: no record (failed INSERT rolled back by SQLite)
- Action: log error, skip file
- Recovery: same as 1.2

### 1.4 CRASH after successful INSERT

- DISK[original]: untouched, no .balance
- DB[status]: 'pending', DB[orig_*] recorded
- Recovery: status='pending' + no .balance + original exists → verify
  original matches orig_size/orig_mtime. If yes: delete DB record, re-process.
  If original changed: mark 'failed', log warning.

---

## PHASE 2: Copy

```bash
sqlite3 "$DB" "UPDATE files SET status='copying' WHERE path='$escaped_path'"
cp_output=$(cp --reflink=never -ax "$main_file" "$tmp_file" 2>&1)
cp_exit=$?
```

### What cp does internally (from coreutils source):

```
open(src, O_RDONLY)
open(dst, O_WRONLY|O_CREAT|O_TRUNC)    ← creates file, truncates to 0
loop: read(src, buf, 256KB) → write(dst, buf, N)
utimensat(dst, timestamps)              ← copies mtime/atime
fchown(dst, uid, gid)                   ← copies ownership
fchmod(dst, mode)                       ← copies permissions
close(dst)
close(src)
# NO fsync(). NO fdatasync(). NEVER. (confirmed from source)
```

After cp returns exit 0: .balance data is in PAGE CACHE (RAM), NOT on disk.
On ZFS: data waits for TXG commit (~5 seconds). This is why we need PHASE 2b.

### 2.1 CRASH before UPDATE to 'copying'

- DISK[original]: untouched, no .balance
- DB[status]: 'pending'
- Recovery: same as 1.4

### 2.2 CRASH after UPDATE, before cp starts

- DISK[original]: untouched, no .balance
- DB[status]: 'copying'
- Recovery: status='copying' + no .balance → cp never started.
  Verify original matches orig_*. If yes: delete DB record, re-process.

### 2.3 cp fails to open original (permission denied, I/O error)

cp has not created .balance yet (opens source first).

- DISK[original]: exists but unreadable
- DISK[.balance]: does not exist
- DB[status]: 'copying'
- Action: log error, UPDATE status='failed'
- Recovery: clean state

### 2.4 cp creates .balance (O_CREAT|O_TRUNC) then CRASH before any write

cp opened dst, created the file, then kill -9 arrives.

- DISK[original]: untouched
- DISK[.balance]: **exists with 0 bytes** (O_TRUNC happened, no write yet)
- DB[status]: 'copying'
- Recovery: status='copying' + .balance exists.
  Check: .balance size (0) != orig_size → PARTIAL. Delete .balance, reset.

### 2.5 CRASH during data copy (mid-write loop)

- DISK[original]: untouched (cp only reads it)
- DISK[.balance]: exists, **PARTIAL** (some bytes written, not all)
- DB[status]: 'copying'
- Note: .balance might have correct CONTENT up to the last written byte,
  but the file is shorter than original. Also: metadata (timestamps, perms)
  NOT yet set (those happen after all data is written).
- Recovery: status='copying' + .balance exists.
  ALWAYS delete .balance. Even if .balance size == orig_size (see 2.6 below).
  Reason: status='copying' means we never entered 'syncing' or 'verifying'.
  We have NO guarantee the data is correct. Delete and re-do.

### 2.6 cp finishes data+metadata, CRASH before status update to 'syncing'

- DISK[original]: untouched
- DISK[.balance]: exists, COMPLETE in page cache. Correct size, perms, mtime.
  BUT: data may not be on physical disk yet (no fsync by cp).
- DB[status]: 'copying' (update to 'syncing' didn't happen)
- Recovery: status='copying' + .balance exists.
  We CANNOT distinguish 2.5 (partial) from 2.6 (complete) reliably.
  Even if .balance size == orig_size, the file might be complete in page cache
  but only partially committed to disk (ZFS TXG not fully committed before
  power loss → on reboot .balance could be shorter than it appeared).
  DECISION: **status='copying' + .balance exists → ALWAYS delete, re-copy.**
  Wasteful (throwing away a potentially good copy) but SAFE.

### 2.7 cp returns error (I/O error, disk full, etc.) - caught by script

- DISK[original]: untouched
- DISK[.balance]: partial or missing
- DB[status]: 'copying'
- Action: rm -f .balance, log error, UPDATE status='failed'
- Recovery: clean state

---

## PHASE 2b: Sync (fsync barrier)

```bash
sqlite3 "$DB" "UPDATE files SET status='syncing' WHERE path='$escaped_path'"
sync "$tmp_file"    # calls fsync() → on ZFS: forces TXG commit
```

This is the critical barrier. After sync returns: .balance data IS on
physical disk (past ZFS TXG, past SLOG, on actual media).

### Why this phase exists

GNU cp never calls fsync. After cp returns 0:
- .balance data: in page cache ✓, on disk ✗ (not guaranteed)
- cmp would read from page cache and say "match" ✓
- If we mv and then lose power: .balance (now original) has 0 bytes on disk
- Result: DATA LOSS

sync forces the data to disk before we verify it. After sync:
- .balance data: in page cache ✓, on disk ✓
- cmp compares data that IS on disk
- Safe to proceed

### 2b.1 CRASH before UPDATE to 'syncing'

- DB[status]: 'copying'
- .balance is COMPLETE in page cache (cp returned 0), unknown on disk
- Recovery: status='copying' → delete .balance, re-copy. (Same rule as 2.5/2.6)

### 2b.2 CRASH after UPDATE, before sync starts

- DB[status]: 'syncing'
- .balance COMPLETE in page cache, NOT guaranteed on disk
- Recovery: status='syncing' + .balance exists.
  .balance is post-cp (complete in page cache when status was set) but
  pre-fsync (might not be on disk).
  DECISION: delete .balance, reset to 'pending'.
  Reason: after reboot, page cache is gone. .balance on disk might be partial.
  We CANNOT verify a file that might be partially on disk.

### 2b.3 CRASH during sync (fsync in progress)

fsync() on ZFS triggers TXG commit. If crash during TXG commit:
- ZFS guarantees: TXG either fully commits or fully rolls back
- If TXG committed: .balance is fully on disk ✓
- If TXG rolled back: .balance is partial/missing on disk

- DB[status]: 'syncing'
- Recovery: same as 2b.2 - delete .balance, re-copy.
  We don't know if the TXG committed. Safe to assume it didn't.

### 2b.4 sync returns, CRASH before status update to 'verifying'

- DB[status]: 'syncing'
- .balance IS on disk (fsync returned = TXG committed)
- But DB still says 'syncing' (update didn't happen)
- Recovery: status='syncing' → per our rule, delete .balance, re-copy.
  This throws away a valid, fsynced copy. Wasteful but SAFE.
  Alternative: check .balance size == orig_size AND re-verify (cmp).
  This is safe (both files on disk, cmp is read-only) but adds complexity.
  DECISION: for simplicity, delete and re-copy. The cost of re-copying
  one file is small compared to the risk of trusting an unverified copy.

### 2b.5 sync returns, status updated to 'verifying'

- DB[status]: 'verifying'
- .balance IS on disk, verified by fsync
- Ready for content verification

---

## PHASE 3: Verify

```bash
sqlite3 "$DB" "UPDATE files SET status='verifying' WHERE path='$escaped_path'"

orig_meta=$(stat -c "%A %U %G %s %Y" "$main_file")
copy_meta=$(stat -c "%A %U %G %s %Y" "$tmp_file")
# compare: exact match, no wildcards
cmp -s "$main_file" "$tmp_file"
```

### What cmp does internally (from coreutils source):

```
open(file1, O_RDONLY)
open(file2, O_RDONLY)
loop: read(fd1, buf1, block_size) + read(fd2, buf2, block_size)
      memcmp(buf1, buf2)
      if different: report and exit(1)
close both
exit(0)    # all bytes matched
```

cmp reads from page cache. After sync (PHASE 2b), page cache matches disk.
cmp is read-only - neither file is modified.

### 3.1 CRASH before stat

- DB[status]: 'verifying'
- .balance on disk (post-fsync)
- Recovery: status='verifying' + .balance exists + original exists →
  RE-VERIFY. Run stat + cmp again. This is safe (read-only).
  If verification passes: proceed to replace.
  If verification fails: delete .balance, mark 'failed'.

### 3.2 stat on original fails (file vanished)

- DISK[original]: gone (deleted by another process)
- DISK[.balance]: exists, on disk, fsynced
- DB[status]: 'verifying', orig_size/orig_mtime recorded
- Action: **ALARM. DO NOT DELETE .balance.**
  .balance might be the only copy of the data.
  Log: "Original vanished. .balance preserved at: $path.balance"
  Log: "MANUAL ACTION: Verify .balance integrity, then: mv .balance original"
  UPDATE status='failed'
- Recovery: manual review. .balance is fsynced, likely intact.

### 3.3 stat on .balance fails (file vanished)

- DISK[original]: exists, untouched
- DISK[.balance]: gone
- DB[status]: 'verifying'
- Action: log error, reset to 'pending' (re-copy on retry)
- Recovery: clean

### 3.4 Metadata mismatch

- DISK: both files exist, metadata differs
- Action: rm -f .balance, UPDATE status='failed'
- Recovery: clean (original untouched)

### 3.5 cmp detects content mismatch

- DISK: both exist, same metadata, different content
- Possible causes: bit flip during cp (ZFS should prevent this but cmp is
  our safety net), or original file was modified after cp started.
- Action: rm -f .balance, UPDATE status='failed'
- Recovery: clean (original untouched)

### 3.6 CRASH during cmp

cmp is read-only. Neither file is modified by cmp.

- DISK[original]: untouched
- DISK[.balance]: on disk (post-fsync), untouched
- DB[status]: 'verifying'
- Recovery: RE-VERIFY from scratch (stat + cmp). Safe (read-only).

### 3.7 Verification passes

- DISK: both files exist, identical content AND metadata
- .balance is fsynced to disk
- DB[status]: 'verifying'
- Next: replace

---

## PHASE 4: Replace (single file, no hardlinks)

```bash
sqlite3 "$DB" "UPDATE files SET status='replacing' WHERE path='$escaped_path'"
mv -f "$tmp_file" "$main_file"
sync "$(dirname "$main_file")"    # force directory change to disk
```

### What rename() does internally:

```
rename(src_path, dst_path):
  - atomically: remove dst directory entry + create new dst entry pointing to src inode
  - decrement old dst inode nlink (if it reaches 0: mark blocks for freeing)
  - src path no longer exists
  - single syscall, atomic from userspace perspective
  - on ZFS: both changes in same TXG (atomic to power failure too)
```

### 4.1 CRASH before UPDATE to 'replacing'

- DB[status]: 'verifying'
- Both files exist, verified
- Recovery: re-verify (3.1), then proceed to replace

### 4.2 CRASH after UPDATE, before mv

- DISK[original]: exists (old inode)
- DISK[.balance]: exists (verified, fsynced)
- DB[status]: 'replacing'
- Recovery: status='replacing' + .balance exists + original exists →
  .balance was verified before status could become 'replacing'.
  Re-verify as safety check, then retry mv.

### 4.3 mv returns error

- DISK: both files untouched (rename failed)
- DB[status]: 'replacing'
- Action: log error + MANUAL ACTION. DO NOT delete .balance.
- Recovery: manual mv

### 4.4 CRASH during rename() syscall

rename() on ZFS is in a single TXG. TXG is atomic.

**Case A: TXG committed (rename happened)**
- DISK[original]: new inode (rebalanced)
- DISK[.balance]: doesn't exist (was renamed)
- Verify: `stat -c %i original` != orig_inode → rename happened

**Case B: TXG rolled back (rename didn't happen)**
- DISK[original]: old inode
- DISK[.balance]: exists (fsynced, data survived)
- Verify: `stat -c %i original` == orig_inode → rename didn't happen

- DB[status]: 'replacing'
- Recovery: check current inode of original.
  - If != orig_inode → Case A. Mark 'done'.
  - If == orig_inode → Case B. Re-verify .balance, retry mv.

### 4.5 mv succeeds, CRASH before sync dir

- DISK[original]: new inode (in page cache / uncommitted TXG)
- DB[status]: 'replacing'
- On power loss: ZFS TXG might roll back rename. Same as 4.4 Case B.
- Recovery: same as 4.4 - check inode, handle accordingly.
- Note: this is why sync dir exists - to force the rename TXG to commit.

### 4.6 mv + sync dir succeed, CRASH before status='done'

- DISK[original]: new inode, confirmed on disk
- DB[status]: 'replacing'
- Recovery: status='replacing' + no .balance + current_inode != orig_inode →
  rename happened and is durable. Mark 'done'.

---

## PHASE 5: Replace (hardlink group)

For a group [main, link1, link2]:

```bash
# 5a: Atomic replace main file
sqlite3 "$DB" "UPDATE files SET status='replacing' WHERE path='$main_escaped'"
mv -f "$tmp_file" "$main_file"
sync "$(dirname "$main_file")"

# 5b: Relink other paths atomically
sqlite3 "$DB" "UPDATE files SET status='relinking'
    WHERE link_main='$main_escaped' AND path != '$main_escaped'"
for link_path in "${paths[@]:1}"; do
    ln -f "$main_file" "$link_path"    # atomic: temp + rename internally
done
```

### What ln -f does internally (from coreutils source):

```
linkat(main_fd, tmpname)         # create temp hardlink "CuXXXXXX"
renameat(tmpname, destination)   # atomic replace destination with temp
# result: destination now points to main's inode
# at NO point does destination not exist
```

### 5a: mv of main file

Identical to PHASE 4. After mv + sync:
- main_file → new inode (nlink=1)
- link1 → old inode (nlink = N-1, because main was renamed away)
- link2 → old inode
- Old inode data is ALIVE because link1, link2 still reference it

### 5b.1 CRASH before UPDATE to 'relinking'

- DB[status]: 'replacing' for all group members
- main has new inode, old links still valid
- Recovery: status='replacing' for main, no .balance → rename happened.
  For group members: find all WHERE link_main = main, check each path.

### 5b.2 CRASH during ln -f on link1

ln -f does linkat(temp) + renameat(temp, link1).

**If crash after linkat but before renameat:**
- link1: still points to old inode (valid data)
- CuXXXXXX temp file: exists, points to new inode
- Recovery: link1 still works (old inode). Temp file is orphan, cleaned up.
  Redo ln -f on link1.

**If crash after renameat:**
- link1: now points to new inode ✓
- Recovery: link1 is correct. Continue with link2.

**Key: at no point does link1 not exist.** Either old inode or new inode.

### 5b.3 CRASH between link1 and link2 processing

- main: new inode ✓
- link1: new inode ✓ (ln -f completed)
- link2: old inode (not yet processed)
- DB[status]: 'relinking'
- Recovery: for each path in group, check inode:
  - Matches main's new inode → already relinked ✓
  - Different from main's inode → needs ln -f
  - Path doesn't exist → needs ln -f from main (shouldn't happen with ln -f,
    only with old rm+ln approach)

### 5b.4 All links processed, CRASH before 'done'

- All paths point to new inode ✓
- DB[status]: 'relinking'
- Recovery: check all paths, confirm correct inode, mark 'done'

### 5b.5 ln -f fails (permission denied, parent dir missing)

- main: new inode ✓ (DATA IS SAFE)
- link_path: still points to old inode (ln -f didn't change it)
- Action: log error + MANUAL ACTION: `ln -f main_file link_path`
  Mark specific path as 'failed'. Continue with other links.

---

## PHASE 6: Record completion

```bash
new_inode=$(stat -c %i "$main_file")
sqlite3 "$DB" "UPDATE files SET status='done', new_inode=$new_inode,
    passes=passes+1, processed_at=datetime('now') WHERE path='$escaped_path'"
```

### 6.1 stat fails (file vanished after mv)

Impossible under normal operation (we just mv'd to this path).
- Action: ALARM. UPDATE status='failed'.

### 6.2 UPDATE fails (SQLite busy/full)

- DISK: file correctly rebalanced
- DB: status still 'replacing' or 'relinking'
- Action: retry. If still fails, log error.
- Recovery: on next run, status='replacing' recovery detects rename happened
  (no .balance, inode changed), marks 'done'. Self-healing.

### 6.3 CRASH after stat, before UPDATE

- Same as 6.2 recovery. Self-healing.

### 6.4 UPDATE succeeds

- DB[status]: 'done'
- Terminal state. File fully processed and recorded.

---

## PHASE R: Recovery on startup

Executed at the beginning of every run, before processing any files.

### R.1 Find crashed runs

```sql
SELECT id, pid, target FROM runs WHERE status = 'running';
```

For each: `kill -0 $pid 2>/dev/null` - if false, PID is dead = crash.

### R.2 Restore dir_mtimes for crashed runs

```sql
SELECT path, mtime FROM dir_mtimes WHERE run_id = $crashed_run_id;
```

For each directory: if exists, `touch -m -d "@$mtime" "$path"`.
Then DELETE dir_mtimes records for that run.

### R.3 Recover files with incomplete status

```sql
SELECT path, status, orig_size, orig_mtime, orig_inode, link_main
FROM files WHERE run_id = $crashed_run_id AND status NOT IN ('done', 'failed');
```

For each file, based on status:

#### status = 'pending'

Nothing happened to this file.
- Does original exist?
  - YES: does it match orig_size/orig_mtime?
    - YES: DELETE from files table. Will be re-processed normally.
    - NO: file changed since we recorded it. Mark 'failed', log warning.
  - NO: file was deleted by something else. Mark 'failed', ALARM.

#### status = 'copying' or 'syncing'

cp was in progress, or completed but fsync didn't happen/complete.
.balance is UNTRUSTED (never verified, possibly not on disk).

- Does .balance exist?
  - YES: **DELETE .balance unconditionally.** We never trust an unverified copy.
  - NO: nothing to clean up.
- Does original exist and match orig_size/orig_mtime?
  - YES: DELETE from files table. Re-process normally.
  - NO: ALARM. Something modified or deleted the original.

#### status = 'verifying'

cp completed, fsync completed, verification was in progress.
.balance is fsynced but verification didn't finish.

- Does .balance exist?
  - YES + original exists: **RE-VERIFY.** Run stat + cmp.
    - Verification passes: proceed to PHASE 4 (replace).
    - Verification fails: delete .balance, mark 'failed'.
  - YES + original MISSING: **ALARM. DO NOT DELETE .balance.**
    .balance is fsynced, likely the only copy. Log for manual review.
  - NO + original exists: .balance was cleaned up. DELETE record, re-process.
  - NO + original MISSING: **CRITICAL ALARM.** Both files gone. Log for investigation.

#### status = 'replacing'

rename() was in progress. Atomic on ZFS (either happened or didn't).

- Check current inode of original: `stat -c %i "$path"`
  - current_inode != orig_inode:
    - Rename HAPPENED. .balance should not exist.
    - Does .balance exist? If yes, something weird - log warning.
    - Mark 'done' with new_inode = current_inode.
  - current_inode == orig_inode:
    - Rename DID NOT happen. .balance should exist.
    - Does .balance exist?
      - YES: re-verify .balance (stat + cmp). If passes: retry mv + sync dir.
      - NO: .balance gone, original has old inode. DELETE record, re-process.
  - original doesn't exist:
    - **IMPOSSIBLE with rename()** (it's atomic). CRITICAL ALARM.

#### status = 'relinking'

Main file was replaced, hardlinks being updated.
Main file is SAFE (rename happened before relinking starts).

- Get main file: SELECT path FROM files WHERE link_main IS NULL (or link_main = path)
  for this group.
- Verify main has new inode (should - replacing was done before relinking).
- For each group member (WHERE link_main = main_path):
  - Does path exist on disk?
    - YES: check inode.
      - Matches main's inode → already relinked ✓. Mark 'done'.
      - Different inode → old link. Run `ln -f main path`. Mark 'done'.
    - NO: path missing. Run `ln -f main path` to recreate. Mark 'done'.

### R.4 Scan for untracked .balance files

```bash
find "$target" -name "*.balance" -type f -print0
```

For each .balance not in the files table (created by external process or
very old crash not tracked in DB):

- Check if original (without .balance suffix) exists:
  - YES: log info. Suggest `rm "$balance_file"`.
  - NO: log WARNING. Suggest `mv "$balance_file" "$original_path"` after manual review.

**Never auto-fix untracked orphans.** We have no orig_* metadata to verify against.

---

## Summary of invariants

Always true if script and recovery work correctly:

1. **A file is NEVER deleted without a verified, fsynced copy existing.**
   cp creates copy → sync forces to disk → cmp verifies → mv replaces.

2. **status='copying' or 'syncing' → .balance is UNTRUSTED.**
   Always deleted during recovery. Never promoted to original.

3. **status='verifying' → .balance is on disk (post-fsync), re-verification safe.**
   cmp is read-only, can be repeated safely.

4. **status='replacing' → check inode to determine rename outcome.**
   rename() is atomic on ZFS (TXG). Current inode tells us definitively.

5. **status='relinking' → main file is ALWAYS safe.**
   mv of main happened before relinking starts. ln -f is per-path atomic
   (temp + rename internally). No path is ever absent.

6. **Every recovery decision uses orig_size, orig_mtime, orig_inode from DB.**
   No assumptions. Compare current disk state against recorded ground truth.

7. **No automatic action on untracked .balance files.**
   Without DB metadata, we can't verify. Manual review only.

---

## What we do NOT protect against

1. **Bit rot before cp** - if original data is silently corrupted on disk,
   we faithfully copy corrupt data. ZFS checksums should catch this, not us.

2. **SQLite DB corruption** - if the .db file is corrupted (disk error on
   ext4 root), tracking state is lost. DATA FILES are unaffected.
   Mitigation: periodic `sqlite3 db ".backup db.bak"` (every 10000 files).

3. **RAM corruption** - if RAM flips a bit during cp or cmp, the copy could
   be silently corrupted. ECC RAM mitigates this. Not our responsibility.

4. **Disk firmware lying about flush** - if the disk acknowledges fsync but
   doesn't actually persist data, we can't detect this. Known issue with
   cheap consumer SSDs. Use enterprise drives.

5. **External process modifying original during cp** - produces a copy with
   mixed old/new data. cmp catches this (original changed between cp read
   and cmp read). File marked 'failed'. No data loss.
