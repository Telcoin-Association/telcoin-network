# Porting `tn-lmdb` to another OS

`tn-lmdb` builds on Linux and macOS only. `lib.rs` refuses every other target with a
`compile_error!`: one off unix, one on every unix but those two. The reason is not the code, which
is plain POSIX almost everywhere. It is that the engine's crash safety rests on what the kernel
does with pages written through a shared mapping, and kernels differ there. macOS showed how much:
its `fsync` leaves some such pages unwritten, which lost whole databases at a power cut until the
engine synced them by `msync` ([README, Platform notes](README.md#platform-notes)).

This file says what a port takes: the OS services the engine relies on (the contract), every
place the code switches on the OS, what is known of each BSD, illumos and Windows, and how to
prove a port before the build refusal is lifted.

**What is tested, and what is not.** Linux (ext4, NVMe) and macOS (APFS, Darwin 24) are tested:
the full suites, and on macOS power-cut snapshots of the device. Everything this file says about
another OS comes from its man pages or its kernel source, and is marked so. None of it has been
run. Treat it as the list of things to check, not as answers.

## Contents

1. [Checklist](#checklist)
2. [The OS contract](#the-os-contract)
3. [Every platform switch](#every-platform-switch)
4. [Per OS: the BSDs and illumos](#per-os-the-bsds-and-illumos)
5. [Windows](#windows)
6. [Verifying a port](#verifying-a-port)

---

## Checklist

The steps, in order. Each links to its section.

1. Read [the OS contract](#the-os-contract) and answer each item for the new OS and every file
   system it must support. Write the answers into [Per OS](#per-os-the-bsds-and-illumos).
2. Pick the sync rule for the OS ([durable syncs](#durable-syncs-of-mapped-pages)): Linux's, macOS's,
   or a new one.
3. Write the OS's arm of each `sys.rs` function that has none: `allocate_range` and
   `read_boot_id` exist for Linux and macOS only, so a new `target_os` does not compile without
   them.
4. Extend each `cfg(target_os = "macos")` switch the OS needs
   ([every platform switch](#every-platform-switch)).
5. Fix the errno and flag differences ([errors](#errors), [open flags](#open-flags-and-file-identity)).
6. Run the suites on the OS, on each file system it must support
   ([verifying a port](#verifying-a-port)).
7. Run the device-level power-cut test ([the device test](#the-device-test)). It is the only test
   of the kernel.
8. Run the regression bench on the sync path ([the cost](#the-cost)).
9. Lift the `compile_error!` for that `target_os`, and update this file and the README's
   Platform notes.

---

## The OS contract

Each item says what the engine needs, where the code uses it, and what Linux and macOS do.

### A shared mapping reserved past the end of file

**What it needs.** A `MAP_SHARED`, `PROT_READ | PROT_WRITE` mapping of the page file that is
longer than the file. The file then grows underneath the mapping, and the pages between the old
and the new end of file must become usable through the same mapping, with no remap. An access to
a page past the end of file may fault; the engine never makes one.

**Where.** `file::map_reservation` maps `max(EnvOptions::map_reserve, file length, one OS page)`
bytes (the reservation, 1 GiB by default) with `memmap2`'s `MmapOptions::len(..).map_raw`.
`PageFile::grow` extends the file (`sys::allocate_range`) and keeps the mapping. A growth past the
reservation calls `PageFile::remap`: a new reservation of at least twice the size. The old
mapping is retired, not unmapped (`PageFile::retired`): a reader that loaded the old base keeps
reading live memory until the `PageFile` drops. So two mappings of one file live at once, and both
must see the same pages. A read-only open maps the file once, `PROT_READ`, at its length
(`ReadOnlyFile::from_file`).

**Linux, macOS.** Both allow it, as POSIX does. Tested by `mapping_base_is_stable_inside_the_reservation`
and `slice_survives_a_remap_past_the_reservation` (`file.rs`).

### Mapping and `pread` coherence

**What it needs.** Bytes written through one mapping must be visible to `pread` on the file, and
bytes written by `pwrite` must be visible through a later mapping. That is a unified buffer cache.

**Where.** Two places use the file outside the mapping. `env::create` writes a new file's two
metas with `pwrite` (`FileExt::write_all_at`). `env::read_record` reads the metas with `pread`
(`read_exact_at`) at open step 4, before the file is mapped. Every later read and write, a
commit's meta included (`Writer::copy_meta`), goes through the mapping. So an open reads, by
`pread`, metas an earlier writer wrote through its mapping, perhaps a writer of this boot that
crashed before any sync. The tests also damage files with `pwrite` and read them with `pread`
while an environment may hold a mapping.

**Linux, macOS.** One page cache: coherent.

### Durable syncs of mapped pages

**What it needs.** Three syncs of pages dirtied through the mapping. When each returns, the pages
it covers are on the device, and so is whatever metadata a reader after a crash needs to find
them (a block allocated for a page, a copy-on-write file system's new block tree).

| sync | used by | Linux | macOS |
|---|---|---|---|
| one range | commit step 4 for one merged range (`DataSync::Range`, `PageFile::msync_range`); step 6's meta page (`PageFile::msync_page`) | `msync(MS_SYNC)` | `msync(MS_SYNC)` |
| many ranges | step 4 for a scattered commit (`data_sync`) | one `fdatasync` (`DataSync::File`, `PageFile::fdatasync_all`) | `msync(MS_ASYNC)` of each merged range, then `msync(MS_SYNC)` of each (`DataSync::Ranges`, `PageFile::msync_ranges`) |
| whole file | `PageFile::fdatasync_all` | `fdatasync` | `msync(MS_SYNC)` of the whole file, then `fsync` |

Every `msync` goes through `PageFile::msync_flags`, which rounds the start down to the OS page.
The plain file syncs are `sys::fsync_file` and `sys::fdatasync_file`: on Linux std's
`File::sync_all` and `sync_data`, on macOS a plain `fsync` (std's calls issue `F_FULLFSYNC` there,
10 to 20 ms each, a guarantee no commit gives). Never call `File::sync_all` or `sync_data` in the
crate.

A failed sync poisons the file: every later write or sync is `Error::Failed`, and nothing is
retried (`PageFile::poison`). A retried `msync` after a writeback error can report success on
Linux with the data lost.

**Why it must be exact.** A meta is written only after its pages are durable. A meta that reaches
the disk naming pages that did not is a lost database, not a rollback: on macOS, before the
engine synced by `msync`, every such snapshot opened `Corrupt` with no key readable. Two more
rules lean on these syncs:

- Open's check of a trusted weak head skips every page written by the last sync
  ([README, "Why the check may skip what was written by the last sync"](README.md#open-and-recovery)).
  That holds only if every sync writes every page stamped since the previous one.
- After a reboot, open trusts the newest steady meta without any check. So "durable" must mean
  durable at a power cut, at least up to the drive's write cache.

**Linux, macOS.** On Linux, `msync(MS_SYNC)` and `fdatasync` each run the file system's fsync
path, drive-cache flush included, and `fdatasync` writes every page dirtied through a mapping.
On macOS `fsync`, `fdatasync`, `F_FULLFSYNC` and `F_BARRIERFSYNC` all left unwritten the pages
dirtied through a mapping in the first 16 MiB of any wider dirty extent (device-tested); every form
of `msync` wrote them. `msync` on macOS does not flush the drive's cache (only `F_FULLFSYNC`
does, which the crate never issues), so a power cut there can lose what that cache held
([README, Platform notes](README.md#platform-notes)).

### A durable size after growth

**What it needs.** A call that makes a grown or shortened file size durable.

**Where.** A growth marks the size unsynced (`PageFile::grow`), and commit step 5 syncs it once
(`PageFile::sync_size_if_grown`, an `fdatasync_file`); `fdatasync_all` covers it too. The close's
trim (`PageFile::trim_to`) and the cut of an inherited tail (in `PageFile::grow`) mark it too.

**Linux, macOS.** `msync` does not make a size durable on either; `fdatasync` and `fsync` do.
On macOS a device test confirmed it: data stored into new space reached the device by `msync`,
but the file ended at its old size until an `fsync`.

### The directory sync

**What it needs.** An `fsync` of a directory that makes a rename in it durable.

**Where.** `sys::fsync_directory`: open the directory read-only, then `sys::fsync_file`. Used
after `env::create` renames `<path>.creating` over the path, and at open step 7. As in
PostgreSQL, an `EACCES` from the open and an `EINVAL`, `ENOTSUP` or `EOPNOTSUPP` from the fsync
are skipped with a warning: such a directory never could make a name durable.

**Linux, macOS.** Both support it.

### A crashed writer's pages, made durable at open

**What it needs.** After a process crash in the same boot, the pages the dead writer dirtied
through its mapping are still in the page cache. A writer open must write them to the device
before it builds on them (they may be a trusted weak head's pages, or a meta whose `msync` never
finished).

**Where.** `Env::open` step 7 calls `sys::fsync_file` on the file before mapping it. On macOS it
then msyncs its whole new mapping (step 8): every mapping of a file shares its pages, so that
writes the dead writer's.

**Linux, macOS.** On Linux the `fsync` writes them. On macOS it does not, and the `msync` does
(`a_writer_open_writes_the_pages_another_mapping_left_dirty`). A read-only open syncs nothing.

### Growth with real blocks, and the trim at close

**What it needs.** A call that reserves real disk blocks for a range at the end of the file and
extends the file over it, and that returns an error on a full disk without extending the file
over unreserved blocks. And a truncation of a file that is still mapped.

**Why.** A store through the mapping into a hole needs a block at its first write fault. On a
full disk the kernel then delivers `SIGBUS`, and there is no Rust error path for it: the process
dies. With the blocks reserved at growth, a full disk is an `io::Error` at the growth instead.

**Where.** `sys::allocate_range(file, from, to)`, with `from` the file's current length. Linux:
`fallocate` mode 0. macOS: `F_PREALLOCATE` (`F_ALLOCATECONTIG | F_ALLOCATEALL`, then
`F_ALLOCATEALL` alone, from the physical end of file), a check that `fst_bytesalloc` covers the
range (else `ENOSPC`), then `set_len`. Where the file system cannot preallocate (`EOPNOTSUPP` or
`ENOSYS` on Linux, `ENOTSUP` on macOS), it falls back to a sparse `set_len`, so the file still
works without the guarantee. The close trims the preallocated tail back
(`Writer::trim_tail`, `PageFile::trim_to`: `set_len` to the pages in use rounded up to the OS
page) while the mapping stays. The first growth of a reopened writer cuts a tail it did not
preallocate the same way (`PageFile::distrust_tail`). Both are `ftruncate` of a mapped file,
which POSIX allows; nothing touches a byte past the new end.

**Linux, macOS.** Both, tested by `allocate_range_reserves_real_blocks` and
`ensure_len_grows_geometrically_with_real_blocks` (each asserts `st_blocks` covers the range, so
run the suite on a file system that preallocates). Even with real blocks, a copy-on-write file
system or a file sharing blocks needs new space at every rewrite
([README, Platform notes](README.md#platform-notes)).

### A failed I/O on a mapped page

**What it needs.** Nothing to handle: the engine has no signal handler. A read error under the
mapping (`EIO`), a store into a hole on a full disk, or an access past the end of file is a
`SIGBUS` on Linux and macOS, and the process dies. Recovery is then a crash recovery at the next
open. A port must make sure such a failure stops the process rather than return wrong bytes.

### The open lock (`flock`)

**What it needs.** A non-blocking, advisory lock with shared and exclusive modes that:

- belongs to the open file description, so a second `open` of the file in the same process is
  excluded (`flock_exclusive_excludes_a_second_open`; open step 3 relies on it: "a second open in
  the same process fails too");
- is shared by every descriptor of that description, `fork`'s copies included, and released when
  the last one closes or the process dies;
- leaves `pread`, `pwrite` and the mapping alone (advisory: the engine's own I/O through other
  descriptors must not block).

**Where.** `sys::flock_exclusive` (a writer's open, and `create`'s lock of `<path>.creating`) and
`sys::flock_shared` (a read-only open), each `flock(LOCK_EX or LOCK_SH | LOCK_NB)`; a
`WouldBlock` error is `Ok(false)`, which open turns into `Error::Locked`. std opens every file
close-on-exec, so an exec'd child holds no lock.

**Why not POSIX `fcntl` record locks.** They belong to the process, not to the open file
description. A second open in the same process would get the lock, so two environments in one
process would both write the file. And closing any descriptor of the file drops all of the
process's locks on it. Open file description locks (`F_OFD_SETLK`, where the OS has them) behave
like `flock` and would do.

**Linux, macOS.** `flock` is per open file description on both. It is local to a host: keep the
file on a local file system ([README, Platform notes](README.md#platform-notes)).

### The boot id (optional)

**What it needs.** 16 bytes that are the same for the whole of one boot and differ between any
two boots. They must never repeat: a repeat lets an open after a reboot trust a weak meta whose
pages the reboot may have lost (open's check of a weak head, `check::check_since`, then stands
between that head and the open; it is a backstop, not the design).

**Where.** `sys::boot_id` caches `read_boot_id`, parsed by `parse_uuid` from UUID text. A
`NoSync` commit's weak meta carries it, and open trusts a weak meta only when its id is the
running boot's (`meta::choose_meta`). `None` (unreadable, unparsable or the nil UUID) is safe: no
weak meta is trusted, so a process crash recovers the last sync, as a reboot does.

**Linux, macOS.** Linux reads `/proc/sys/kernel/random/boot_id`, macOS
`sysctlbyname("kern.bootsessionuuid")`, each a UUID string (`the_host_boot_id_is_known_and_stable`).
A boot time is not a safe substitute: a stepped clock moves it within a boot (safe: weak heads are
then distrusted), and a host without a working clock can repeat it across boots (unsafe).

### The OS page size and `msync` alignment

**What it needs.** The OS page size, a power of two of at least 4 KiB.

**Where.** `sys::os_page_size` (`sysconf(_SC_PAGESIZE)`, 4096 if that fails). `msync` needs a
page-aligned start, so `PageFile::msync_flags` rounds down to it. Growth targets round up to it,
and so does the close's trim length (`Writer::trim_len`). The engine's own page size
(`EnvOptions::page_size`, 4 KiB by default) can be below it: Apple Silicon's OS pages are
16 KiB, so both metas then share one OS page ([README, Platform notes](README.md#platform-notes)).
Tested by `os_page_size_is_a_power_of_two`.

### Access advice (performance only)

**What it needs.** Nothing for correctness. `madvise(MADV_RANDOM)` is a hint, and a failure is
logged and ignored.

**Where.** `file::advise_access`, through `memmap2`'s `advise(Advice::Random)`, on every mapping
unless `EnvOptions::readahead` is set. `readahead` defaults to `!cfg!(target_os = "linux")`. On
Linux the advice keeps commits from writing back whole large folios. On macOS it saved nothing at
commit and slowed first writes into new space, so macOS gives no advice. A port sets its default by
measurement (the regression bench), and `every_mapping_follows_the_readahead_option` must then
follow it.

### Errors

The engine reads a few errno values. Each is a place to check on a new OS:

| where | errno | meaning |
|---|---|---|
| `file::mapped` | `ENOMEM` | a refused reservation is `Error::OutOfMemory`, anything else `Error::Io` |
| `sys::allocate_range` | Linux `EOPNOTSUPP`, `ENOSYS`; macOS `ENOTSUP` | no preallocation here: fall back to a sparse `set_len`. Every other error (`ENOSPC`, `EDQUOT`, `EFBIG`, `EIO`) fails the growth |
| `sys::fsync_directory` | `EACCES` (open), `EINVAL`, `ENOTSUP`, `EOPNOTSUPP` (fsync) | skipped with a warning |
| `sys::retry_interrupted` | `EINTR` (`ErrorKind::Interrupted`) | the call is issued again |
| `sys::flock` | `EWOULDBLOCK` (`ErrorKind::WouldBlock`) | the lock is held: `Locked` |
| `env::create` | `ELOOP`, `EISDIR` | `<path>.creating` is a symbolic link or not a regular file: refused, `InvalidInput` |

The adapter crate `tn-lmdb-reth` reports an `Error::Io`'s OS code and names `libc::ENODATA`,
`EACCES`, `EINVAL`, `ENOMEM` and `EIO` (`src/error.rs`). `libc` has no `ENODATA` for FreeBSD,
OpenBSD or DragonFly, so that crate does not build there as it is.

### Open flags and file identity

- `env::create` opens `<path>.creating` read-write, create, no truncate, with `O_NOFOLLOW`
  (`OpenOptionsExt::custom_flags`), then locks it and truncates it.
- `env::open_locked` opens the path read-write, following symbolic links by design.
- `env::open_shared` (read-only) opens it `O_RDONLY | O_NONBLOCK`, so a FIFO put at the path
  fails at once instead of blocking.
- After the lock, `open_locked`, `open_shared` and `env::remove_unused` compare the locked file's
  `(dev, ino)` with the path's (`MetadataExt`). The OS must give a file a stable `(dev, ino)`.

### Fork and exec

An environment belongs to the process that opened it. `EnvInner` records `std::process::id()`,
and its drop does nothing in another process: no final sync, no trim
(`a_forked_childs_drop_leaves_the_parents_file_alone`). A forked child shares the parent's open
file description, and with it the lock. An exec'd child holds nothing, since std opens files
close-on-exec ([README, Concurrency rules](README.md#concurrency-rules)).

---

## Every platform switch

Every `cfg(target_os = ...)`, `cfg(unix)` and `cfg!(target_os = ...)` site in `tn-lmdb` and its
adapter crate, by the item it sits in (find them with
`grep -rn 'target_os\|cfg(unix\|cfg(not(unix' crates/tn-lmdb crates/tn-lmdb-reth`). A new OS
falls into each `not(target_os = "macos")` arm, that is Linux's, unless the switch is extended.

### In the library

| file | item | switch | what it switches |
|---|---|---|---|
| `lib.rs` | crate | `not(unix)` | `compile_error!`: no mapping, `msync` or `flock` ([Windows](#windows)) |
| `lib.rs` | crate | `all(unix, not(any(linux, macos)))` | `compile_error!`: syncs verified on Linux and macOS only |
| `sys.rs` | `read_boot_id` | `linux` | reads `/proc/sys/kernel/random/boot_id` |
| `sys.rs` | `read_boot_id` | `macos` | `sysctlbyname("kern.bootsessionuuid")` |
| `sys.rs` | `fsync_file` | `macos` / `not(macos)` | plain `libc::fsync` (retried on `EINTR`) / `File::sync_all` |
| `sys.rs` | `fdatasync_file` | `macos` / `not(macos)` | `fsync_file` / `File::sync_data` |
| `sys.rs` | `allocate_range` | `linux` | `fallocate` mode 0, sparse `set_len` fallback |
| `sys.rs` | `allocate_range` | `macos` | `F_PREALLOCATE` then `set_len`, sparse fallback |
| `env.rs` | `DataSync::Ranges` | `macos` | the variant exists on macOS only |
| `env.rs` | `DataSync::File` | `cfg_attr(all(macos, not(test)))` | `allow(dead_code)`: only a test knob picks it on macOS |
| `env.rs` | `EnvOptions::default` | `!cfg!(linux)` | `readahead` default: off (advise `MADV_RANDOM`) on Linux only |
| `env.rs` | `Writer::make_durable` | `macos` (test only) | `data_by_file` also reports `DataSync::Ranges` |
| `env.rs` | `Writer::make_durable` | `macos` | the `DataSync::Ranges` arm: `msync_ranges(merged_ranges(..))` |
| `env.rs` | `data_sync` | `macos` / `not(macos)` | a scattered commit is `Ranges` / `File` |
| `env.rs` | `merged_ranges` | `macos` | the ranges `Ranges` msyncs |
| `env.rs` | `Env::open` | `macos` | `msync_range(0..len)` of the new mapping (step 8) |
| `file.rs` | `PageFile::msync_ranges` | `macos` | `MS_ASYNC` pass, then `MS_SYNC` pass |
| `file.rs` | `PageFile::fdatasync_all` | `macos` | `msync(MS_SYNC)` of the whole file before the `fdatasync` |

`read_boot_id` and `allocate_range` have no fallback arm. Once the `compile_error!` is lifted for
a new OS, the build fails at both: that is deliberate, so a port must write them. For
`x86_64-unknown-freebsd`, `cargo check -p tn-lmdb --tests --target x86_64-unknown-freebsd`
reports exactly these: the `compile_error!` and the two missing functions; the rest of the
library and its tests type-check there.

### In tests

| file | item | switch | what |
|---|---|---|---|
| `file.rs` | `PageFile::dirty_os_pages` | `all(test, macos)` | counts modified pages with `mincore` (`MINCORE_MODIFIED`, `MINCORE_MODIFIED_OTHER`: Apple only) |
| `file.rs` | `fdatasync_all_writes_every_page_dirtied_through_the_mapping` | `macos` | no dirty page after `fdatasync_all` |
| `file.rs` | `a_failed_msync_of_ranges_poisons_the_file` | `macos` | `msync_ranges` (macOS only) keeps the poison rule |
| `env/tests.rs` | `dirty_pages_form_one_range_or_take_one_fdatasync` | `macos` / `not(macos)` | the expected plan for a scattered commit |
| `env/tests.rs` | `a_scattered_durable_commit_leaves_no_page_dirty` | `macos` | by `dirty_os_pages` |
| `env/tests.rs` | `a_writer_open_writes_the_pages_another_mapping_left_dirty` | `macos` | by `dirty_os_pages` |
| `env/tests.rs` | `scattered_pages_msync_their_merged_ranges` | `macos` | `merged_ranges` |
| `env/tests.rs` | `every_mapping_follows_the_readahead_option` | `cfg!(linux)` | the `readahead` default |
| `env/tests.rs` | `every_mapping_carries_the_random_read_flag_on_linux` | `linux` | `rr` in `/proc/self/smaps` |
| `env/tests.rs` | `an_inherited_tail_is_cut_and_regrown_with_real_blocks` | `linux` | one assert: a `set_len` extension is a hole |
| `tn-lmdb-reth/src/tests/mod.rs` | `mod crash` | `unix` | a child process killed mid-commit, then reopened |

A new OS with macOS's sync rule needs a way to count pages left dirty, as `dirty_os_pages` does
on macOS, or those tests do not run there.

### Other OS dependencies without a switch

These build on any unix and are the work list for a non-unix target:

- `std::os::fd::AsRawFd` (`sys.rs`), `std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt}`
  (`env.rs`, `file.rs`, `sys.rs`); `libc` for `msync`, `flock`, `sysconf`, `fcntl`, `fallocate`,
  `O_NOFOLLOW`, `O_NONBLOCK` and the errno values above.
- `memmap2`'s `advise`, which exists on unix only.
- Tests: `FileExt` in most suites (damage by `pwrite`, reads by `pread`); `disk_model.rs` keys
  files by inode (`MetadataExt::ino`); `PermissionsExt` (`0o444` in `tests/read_only.rs` and
  `file.rs`); `symlink`, `FileTypeExt` and `UnixListener` (the symbolic-link and FIFO tests in
  `env/tests.rs`: `a_temporary_file_that_is_not_a_regular_file_is_refused`,
  `a_symlinked_path_opens_only_the_database_it_names`, `a_path_that_is_not_a_regular_file_is_refused`);
  `MetadataExt::blocks` (`allocated`); `libc::fork`, `waitpid` and `_exit` with `ExitStatusExt`
  (`a_forked_childs_drop_changes_nothing_in_the_child`).
- Not OS switches: `target_arch` in `page.rs` (`prefetch`) and `crc.rs`.

---

## Per OS: the BSDs and illumos

Everything here is from man pages or kernel source, read but not run. The sync verdicts come
from each kernel's `fsync`, `fdatasync` and `msync` paths: does `fsync` write pages dirtied
through a mapping, does `msync(MS_SYNC)` write them and wait for the device, does `msync` also
make the size and a page's newly allocated block durable, and do `pread` and the mapping see the
same pages.

| OS (file system) | `fsync` writes mapped pages | `msync(MS_SYNC)` writes and waits | `msync` makes size and new blocks durable | mapping and `pread` coherent |
|---|---|---|---|---|
| Linux (ext4 tested; XFS, btrfs) | yes | yes | yes | yes |
| macOS (APFS) | **no** (device-tested) | yes (device-tested) | no (size: device-tested) | yes |
| FreeBSD (UFS) | yes | yes | yes | yes |
| FreeBSD (ZFS) | yes | yes | likely yes | yes |
| NetBSD (FFS) | yes | yes | **no** | yes |
| OpenBSD (FFS) | **no** | **no**: returns with the pages in the buffer cache | no | **no** |
| DragonFly (UFS) | yes | yes | likely yes | yes |
| DragonFly (HAMMER2) | yes | likely | assume no (copy-on-write) | yes |
| illumos, Solaris (ZFS) | yes | yes | likely yes | yes |
| illumos, Solaris (UFS) | yes | yes | likely no | yes |

Three sync rules follow:

1. **Linux's rule** (one `msync` for one range, else one `fdatasync`; the meta by `msync`): where
   `fsync` writes mapped pages and `msync` is a complete data sync. FreeBSD, and illumos on ZFS
   only.
2. **`fdatasync` after every data sync:** where `fsync` writes mapped pages but `msync` leaves
   new-block metadata behind. Step 4 always takes one `fdatasync` (drop the one-range `msync`),
   and step 6 msyncs the meta page and then `fdatasync`s. NetBSD, DragonFly, illumos (UFS).
3. **`msync` for every mapped page, then `fsync`:** where `fsync` skips mapped pages. macOS's
   arms, plus an `fsync` after each `msync` where `msync` does not wait for the device, plus a
   whole-mapping `msync` at a writer open. OpenBSD would also need the metas read through the
   mapping at open, not by `pread`.

### FreeBSD

- **Syncs (source):** `fsync` and `fdatasync` clean the file's VM object first, so they write
  mapped pages on UFS and ZFS. A ranged `msync(MS_SYNC)` writes synchronously, and on UFS also
  writes the inode, size included; on ZFS it commits the file's intent log. Linux's rule applies.
  Make `fsync_file` and `fdatasync_file` std's calls (as on Linux) and leave the `macos` switches.
- **Growth (docs and source):** `posix_fallocate(2)` (FreeBSD 9.0 and later) guarantees the
  blocks and extends the file. It returns the error number instead of setting `errno`. It is
  documented to fail with `EINVAL` where the file system does not support it and `ENODEV` for an
  unsupported file; in the current source ZFS returns `EOPNOTSUPP`. So `allocate_range` falls
  back on `EINVAL`, `ENODEV` and `EOPNOTSUPP`. UFS uses the generic implementation, which writes
  the blocks. On ZFS nothing preallocates: a full disk then surfaces where ZFS assigns space, at
  writeback, as a failed sync (which poisons the file and latches the environment) or as a fault;
  check which, on a full pool.
- **Lock (docs):** `flock(2)`: "Locks are on files, not file descriptors"; `dup` and `fork` share
  one lock; advisory; `EWOULDBLOCK` with `LOCK_NB`. A `flock` lock is also seen by `fcntl` users.
  The page does not state that a second `open` gets a separate lock: check it with
  `flock_exclusive_excludes_a_second_open`.
- **Boot id (docs):** `kern.boot_id` (added in March 2020, so FreeBSD 13.0 and later): 16 random
  bytes, regenerated at each boot, returned raw (not as UUID text), and `ENXIO` before the random
  generator is seeded. Read it with `sysctlbyname` and take the 16 bytes as they are, refusing all
  zeros; `parse_uuid` does not apply.
- **Advice:** `MADV_RANDOM` exists.
- **Other:** `O_NOFOLLOW` on a symbolic link fails with `EMLINK`, not `ELOOP` (open(2)), so
  `env::create` must map `EMLINK` too. The adapter crate needs `ENODATA` replaced.

### NetBSD

- **Syncs (source):** `fsync` and `fdatasync` write mapped pages (they put all dirty pages first).
  `msync(MS_SYNC)` writes the data blocks and waits, but does not write the inode. And a block for
  a page first stored into a hole is allocated at the write fault, as delayed metadata that only
  an `fsync` writes. Rule 2. Plain FFS `fsync` does not flush the drive's cache either
  (`fsync_range(2)` with `FDISKSYNC` does): decide whether the port needs that.
- **Growth (docs and source):** `posix_fallocate(2)` exists (NetBSD 7.0 and later), but FFS
  implements it as `EOPNOTSUPP`, so growth falls back to a sparse `set_len`. Every page is then a
  hole at its first write, and a full disk is a `SIGBUS`. Rule 2's `fdatasync` covers the delayed
  allocation; a zero-fill by `pwrite` at growth would restore the full-disk guarantee, at the cost
  of writing each growth step once. The `libc` crate does not bind `posix_fallocate` for NetBSD.
- **Lock:** `flock` exists; check that a second open is excluded.
- **Boot id (docs):** none. sysctl(7) documents `kern.boottime` only, which is not safe
  ([the boot id](#the-boot-id-optional)). Return `None`.
- **Advice:** `MADV_RANDOM` exists.
- **Other:** `O_NOFOLLOW` on a symbolic link fails with `EFTYPE` (open(2)).

### OpenBSD

- **Syncs (source):** no unified buffer cache. `fsync` walks the buffer cache only and never sees
  mapped pages. `msync(MS_SYNC)` copies them into the buffer cache (full blocks started, partial
  blocks delayed) and returns without waiting for the device or writing the inode. `pread` reads
  the buffer cache, so it can miss newer bytes in the mapping, and a `pwrite` does not update a
  resident mapped page. Rule 3 with an `fsync` after every `msync` (data and meta), the metas read
  through the mapping (or after `msync(MS_INVALIDATE)`) at open, and `create`'s `pwrite`s
  followed by an invalidating `msync`. This is the largest port of the BSDs; refusing OpenBSD stays
  a fair answer.
- **Growth (docs):** no `posix_fallocate` (man.openbsd.org has no page). Sparse `set_len`, with
  `SIGBUS` on a full disk, unless growth zero-fills by `pwrite` (then mind the coherence above).
- **Lock (docs):** `flock(2)`: "Locks are on files, not file descriptors"; `dup` and `fork` share
  one lock; advisory; `EWOULDBLOCK`.
- **Boot id (docs):** none. sysctl(2) documents `KERN_BOOTTIME` only (`hw.uuid` names the
  machine, not the boot). Return `None`.
- **Advice:** `MADV_RANDOM` exists.
- **Other:** no `ENODATA` in `libc` for the adapter crate.

### DragonFly BSD

- **Syncs (source):** like FreeBSD, `fsync` cleans the VM object first, and `msync(MS_SYNC)`
  writes synchronously. Whether `msync` makes the size durable is not established on UFS
  (likely, as on FreeBSD) and should be assumed not on HAMMER2, which is copy-on-write and needs
  its flush to make new blocks reachable. Rule 2 is the safe choice.
- **Growth (docs):** `posix_fallocate(2)` exists; `EINVAL` where unsupported, `ENODEV` for an
  unsupported file. Which file systems support it is not documented: test UFS and HAMMER2. On
  HAMMER2 a real preallocation is unlikely (copy-on-write).
- **Lock:** `flock` exists; check that a second open is excluded.
- **Boot id:** not found in the docs consulted; assume `kern.boottime` only and return `None`.
- **Other:** no `ENODATA` in `libc` for the adapter crate.

### illumos and Solaris

- **Syncs (source, illumos):** `fsync` writes mapped pages on ZFS and UFS. `msync(MS_SYNC)` is a
  synchronous write of the range; on ZFS it commits the intent log, on UFS it does not write the
  inode or a block allocated at the write fault. Rule 1 on ZFS only; rule 2 if UFS must be
  supported, since the file system is not known at build time.
- **Growth (docs):** `posix_fallocate(3C)` exists; `EINVAL` where the file system does not
  support it, `ENODEV` for a file that is not regular. On ZFS (copy-on-write) a real preallocation
  is unlikely: check what a full pool does, as on FreeBSD.
- **Lock (docs):** illumos `flock(3C)` locks are "owned by open files, not file descriptors",
  conflict with `fcntl` locks, and are not supported over NFS. The `libc` crate does not declare
  `flock` for `target_os = "solaris"`; Solaris needs open file description locks or a check of
  its own `flock`.
- **Boot id:** not checked; the boot time (kstat) is the only source known, and it is not safe.
  Return `None`.
- **Advice:** `MADV_RANDOM` exists.

---

## Windows

**No fundamental blocker.** LMDB runs on Windows with a memory-mapped data file, and every item
of the contract has a Windows counterpart. But the port is the largest one: `sys.rs` and the
mapping half of `file.rs` are rewritten, two contract items (the reservation past the end of file
and the trim of a mapped file) need a different design, and the tests that use unix APIs need
other forms. The facts below are from Microsoft's documentation unless marked otherwise.

| contract item | Windows API | notes |
|---|---|---|
| the mapping | `CreateFileMappingW` (`PAGE_READWRITE`), `MapViewOfFile` (`FILE_MAP_WRITE`) | a view cannot reach past the mapping object's size: see below |
| one range | `FlushViewOfFile` | "initiates writing of dirty pages"; does not wait for the drive's cache and writes no metadata. Docs: call it, then `FlushFileBuffers` |
| many ranges, whole file | `FlushViewOfFile` of each range, then one `FlushFileBuffers` | whether `FlushFileBuffers` alone writes view-dirtied pages is not documented: do not rely on it (macOS's lesson) |
| `fdatasync` | `NtFlushBuffersFileEx(FLUSH_FLAGS_FILE_DATA_SYNC_ONLY)` | WDK docs: data plus "only metadata that is necessary for data retrieval", storage cache flushed; NTFS only, from `ntdll` (the call is Windows 8 and later; check the flag's own minimum version) |
| preallocation | `SetFileInformationByHandle(FileAllocationInfo)` then `FileEndOfFileInfo` (or `SetEndOfFile`) | not verified here: check that a full disk fails the call |
| size sync | `FlushFileBuffers` | writes the file's metadata |
| directory sync | none as such | NTFS journals its metadata; a rename can be made durable with `MoveFileExW(MOVEFILE_WRITE_THROUGH)` (not verified here) |
| lock | `LockFileEx` (shared, or `LOCKFILE_EXCLUSIVE_LOCK`; `LOCKFILE_FAIL_IMMEDIATELY`) | byte-range and per handle: see below |
| boot id | none documented | return `None` first (safe: no weak meta trusted) |
| page size | `GetSystemInfo`: `dwPageSize` (4 KiB) and `dwAllocationGranularity` (64 KiB) | a view's file offset must be a multiple of the allocation granularity |
| `SIGBUS` | `EXCEPTION_IN_PAGE_ERROR` | docs: guard view access with structured exception handling; the engine has none, so the process dies, as with `SIGBUS` |
| access advice | none equivalent to `MADV_RANDOM` (`PrefetchVirtualMemory` is the opposite) | perf only |

**The hard parts:**

- **No reservation past the end of file.** `MapViewOfFile` documents that "all bytes must be
  within the maximum size specified by `CreateFileMapping`", and a mapping object's maximum size
  is the file's size, or extends the file to it ("the file on disk is increased to match the
  specified size"). `memmap2` creates the mapping with a maximum size of 0 (the current size), so
  `MmapOptions::len(reserve).map_raw` past the end of file fails there. `SEC_RESERVE` "has no
  effect" for a file-backed mapping, per the same docs. Two ways out:
  - Remap at every growth, through the path that already exists for a growth past the
    reservation (`PageFile::remap`, retired mappings kept for readers). Growth is geometric, so
    the remaps are few, but every growth then moves the base and keeps one more retired view.
  - Size the file to the whole reservation at open, as LMDB's 0.9 release does on Windows
    (`mdb_env_map`: the mapping object is created at the full map size, which extends the file).
    Simple, but the file on disk is then as large as the reservation, and the growth, the
    preallocation and the trim at close lose their meaning.
  - Use the native section API, as LMDB's development branch does:
    `NtCreateSection(.., SEC_RESERVE, file)` with no maximum size and
    `NtMapViewOfSection(.., MEM_RESERVE)` at the full reservation, so the view reserves address
    space past the end of file. This is native, little-documented behaviour: test it before
    building on it.
- **A mapped file cannot be shrunk.** `SetEndOfFile` documents that every view must be unmapped
  and the mapping object closed first. So the close's trim (`PageFile::trim_to`) must unmap first,
  which it can: it is the close's last step. The cut of an inherited tail at the first growth
  (`PageFile::distrust_tail`, in `PageFile::grow`) cannot unmap while readers hold views; it needs
  another design, such as cutting the tail at open, before the file is mapped.
- **The lock is mandatory and per handle.** `LockFileEx` locks a byte range: an exclusive lock
  denies other processes reads and writes of that range, and access through a second handle in
  the same process is denied too. Check that a second `LockFileEx` through another handle fails:
  that gives the "second open fails" rule. It "does not prevent reading or writing
  from a mapped file view". Lock a range past any data (a range past the end of file is allowed),
  so `create`'s `pwrite` and open's `pread` of the metas are not blocked. Locks are released at
  close or process exit, but "the time it takes ... depends upon available system resources", so
  a reopen right after a crash can see `Locked` briefly.
- **Is `FlushFileBuffers` enough for view pages?** Its docs speak of `WriteFile`'s buffers only,
  and `FlushViewOfFile`'s docs pair the two. The device test must show which call writes pages
  dirtied through a view, as it did on macOS. LMDB's mode that writes through the map, as this
  engine does, syncs by `FlushViewOfFile` of the map, then `FlushFileBuffers` (`mdb_env_sync`);
  its default mode writes pages with `WriteFile` and does not depend on the view at all.
- **Valid data length.** NTFS keeps a valid data length per file, below which bytes were written.
  A write far past it makes NTFS zero-fill the gap first. A growth preallocates blocks but does
  not move the valid data length, so the first page written at the far end of a new growth step
  may cost a zero-fill of the whole step (not measured: check with the regression bench).
  `SetFileValidData` moves it, but needs the `SE_MANAGE_VOLUME_NAME` privilege and exposes old
  disk contents.
- **Coherence.** The docs say views are coherent with each other, but "a mapped view of a file is
  not guaranteed to be coherent with a file that is being accessed by `ReadFile` or `WriteFile`".
  Local files share one cache in practice, but the docs do not promise it: read the metas through
  the mapping at open, or verify.
- **Share modes and rename.** std opens files with `FILE_SHARE_READ | FILE_SHARE_WRITE |
  FILE_SHARE_DELETE` (std source); keep `FILE_SHARE_DELETE`, or `create`'s rename over a path
  another handle holds open fails. std's `fs::rename` asks for no write-through, so `create`
  needs its own durable rename.
- **Unix APIs to replace.** `std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt}` (on
  Windows `std::os::windows::fs::FileExt::seek_read`/`seek_write`, and a file id from
  `GetFileInformationByHandle` in place of `(dev, ino)`), `O_NOFOLLOW` and `O_NONBLOCK`
  (`FILE_FLAG_OPEN_REPARSE_POINT`; no FIFOs), the errno values, `memmap2`'s `advise`, and in tests
  `fork`, `UnixListener`, `symlink` (which needs a privilege on Windows) and `PermissionsExt`.
  `disk_model` keys files by inode and needs the file id instead.

**Rough size.** A new `sys.rs` arm, a Windows `PageFile` mapping and growth path (remap at each
growth is the smaller step), a lock on a byte range, and a test pass that replaces or gates about
a dozen unix-only tests. Then the device test on a VHD. Weeks, not days, mostly in the mapping
and in proving the sync.

---

## Verifying a port

Required before the `compile_error!` is lifted.

### The test suites

On the target, on each file system the port supports:

```text
cargo nextest run -p tn-lmdb -p tn-lmdb-reth
# the heavy variants and the guarded measurement, about 10 min:
cargo test --release -p tn-lmdb -- --ignored --skip tn_lmdb_sync_and_merge --skip stamp_probe
cargo +nightly-2026-03-20 clippy -p tn-lmdb --all-targets --target <target> -- -D warnings
```

A file system that cannot preallocate fails `allocate_range_reserves_real_blocks` and
`ensure_len_grows_geometrically_with_real_blocks` by design: run them on one that can, and say
which file systems the port supports.

### Why the crash suite is not enough

The crash suites (`tests/crash.rs` and the others built on `disk_model`) model the kernel: an
`msync` writes its range to the model's disk, an `fsync` writes every page of the file and its
size, a power loss keeps only what was synced. They test that the engine calls the right syncs in
the right order, given that model. They cannot test that the kernel does what the model says.
macOS passed every crash test while its `fsync` skipped mapped pages; only a test of the device
found it. The model's `fsync` is Linux's (`disk_model.rs`, module docs): a port whose kernel
differs must change the model too, or the crash tests check the wrong thing.

### The device test

A power-cut snapshot: what the device holds right after a sync, read while the process still
runs, is what a power cut at that instant would leave (up to the drive's own cache, which a
snapshot of a disk image does not have).

**The volume.** A file system on a disk image or memory disk whose raw device the test can read
while the file system stays mounted:

| OS | device |
|---|---|
| macOS | `hdiutil create -fs APFS ...`, `hdiutil attach -nobrowse -owners on`: the user owns `/dev/rdiskN` |
| Linux | a file attached with `losetup`, then `mkfs` and `mount` |
| FreeBSD, DragonFly | `mdconfig -a -t vnode -f image` (or `-t swap`), `newfs`, `mount` |
| NetBSD | `vndconfig` on an image, `newfs`, `mount` |
| OpenBSD | `vnconfig` on an image, `newfs`, `mount` |
| illumos | `lofiadm -a image`, `newfs` or a ZFS pool on the lofi device |
| Windows | a VHD (`diskpart` `create vdisk`, `attach vdisk`), formatted NTFS, read as `\\.\PhysicalDriveN` (as administrator) |

**The raw probe.** Write a probe in C (or Rust) that:

1. writes every 4 KiB block of a test file with a self-describing pattern: a magic, a per-run
   random tag, the block's index, a generation number, and a body derived from those, so a scan
   can find and verify any block anywhere on the device;
2. makes that state durable by every means (`pwrite`, `fsync`, the strongest drive flush the OS
   has), maps the file shared, and checks by a scan that the device holds every block at its first
   generation;
3. dirties chosen blocks through the mapping with the next generation (random pages, a contiguous
   span, a mature file rewritten in place);
4. syncs by the call under test (`fsync`, `fdatasync`, `msync` of each range, `msync` of the
   whole file, and the OS's own calls);
5. writes a barrier: one byte to another file on the same volume, flushed with the strongest call;
6. scans the raw device at every 512-byte offset for the magic and tag, verifying each block's
   body, and counts which dirtied blocks are on the device at the new generation. Scan while the
   mapping is still held: an unmap or close can write pages the sync skipped.

Controls first: `pwrite` then `fsync` must show every block; mapped writes with no sync must
show none. Then check the snapshot against the file system: copy the raw device to an image at
the snapshot instant, attach the copy read-only, and read the file through the file system. It
must show exactly what the scan found. Where the OS can count dirty pages (`mincore` with
`MINCORE_MODIFIED` on macOS), that count matched the pages left off the device in every run.

**End to end.** Then the same with the engine: a program opens an environment on the test volume
in `SyncMode::Durable`, commits a known workload (a bulk load, then rounds of scattered rewrites,
then appends into new space, and a commit that grows the file), and after the last commit
returns, writes the barrier and copies the raw device. Detach the volume, attach the copy
read-only, copy the database file out through the file system, and check the copy:
`Env::open_read_only`, `Env::check`, and every key read against the workload's model. Every trial
must open clean at the last commit. On macOS, before the fix, every scattered commit's snapshot
opened `Corrupt` with no key readable; after it, every trial was clean.

**What a snapshot cannot show.** It shows what reached the device, not what a drive's own write
cache would lose at a power cut. That part is the sync call's flush of the drive cache (Linux's
`fsync` path; `F_FULLFSYNC` on macOS, which the engine does not issue), and a disk image has no
such cache to lose.

### The cost

The sync path is on every durable commit. Compare the port's sync rule against the obvious
alternatives with the regression bench, `etc/tn-lmdb-bench-compare.sh` (the working tree against
`HEAD`, `--tier quick`, about 2 minutes; [`tn-lmdb-bench`](../tn-lmdb-bench/README.md) has the
rows), and with the on-demand benchmark of step 4 (`tn_lmdb_sync_and_merge` in `bench.rs`;
[README, Step 4](README.md#step-4-one-msync-or-one-fdatasync)). State the durable rows' ratios.

### Lifting the build refusal

Once all of the above pass:

1. Add the `target_os` to the second `compile_error!`'s list in `lib.rs`.
2. Update this file: the OS's row in the [Per OS](#per-os-the-bsds-and-illumos) table, marked
   tested, and its section with what the tests found.
3. Update the README's [Platform notes](README.md#platform-notes) and the crate docs in `lib.rs`
   to name the new OS.
4. Update the tables in [Every platform switch](#every-platform-switch) for every switch the port
   added.
