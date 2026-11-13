# Execution database: disk and memory

This page covers the execution database of this release: reth's tables on the tn-lmdb engine, in `db/tn-lmdb.dat`.
It adds to [Hardware Requirements](hardware-requirements.md), whose network benchmark ran on the previous execution database (MDBX).

Every figure here is measured, on the setup named beside it.
Most come from the execution benchmark, which runs generated transfer and contract load through the node's execution path with durable commits.
Its **peak** profile puts 20 batches of 30M gas in each consensus output (about 11,700 transactions).
Its **busy** profile puts 8 batches of 15M gas in each output.

## Disk

Each consensus output ends in one durable commit of the execution database.
The commit writes every page the output changed, and those pages are spread across the file.
On the peak profile with storage layout v2 and gap fill off, one commit wrote about 77,000 pages as about 62,600 separate write I/Os, about 4.9 KiB each.
On the busy profile it was about 21,400 write I/Os.
The engine counted these on a Mac M1 Max; the cloud disks below saw writes of about the same size.
On a disk that caps I/Os per second, the commit's time is set by that I/O count, not by its bytes.

Mean time in the commit of one peak output, gap fill off:

| Disk | VM | Layout v2 | Layout v1 |
| --- | --- | --- | --- |
| GCP Hyperdisk Balanced, 4,500 IOPS, 515 MB/s | c3-highmem-8, 64 GB | 12.3 s | 16.4 s |
| GCP Hyperdisk Balanced, 20,000 IOPS, 800 MiB/s | c3-highcpu-8, 16 GB | 1.4 s | 2.4 s |

Mean time per consensus output, everything included (execution, state root, commit), layout v2, gap fill off, serial state root:

| Disk | VM | Peak | Busy |
| --- | --- | --- | --- |
| Local NVMe SSD, ext4 | c3-standard-8-lssd, 32 GB | 2,682 ms | 685 ms |
| GCP Hyperdisk Balanced, 20,000 IOPS, 800 MiB/s | c3-highcpu-8, 16 GB | 3,509 ms | 1,095 ms |

During commits on the 20,000-IOPS disk the node wrote about 90 MiB/s in writes of about 5 KiB, about 18,000 IOPS: the disk was the limit.

### By provisioned IOPS, with the node's defaults

The tables above ran with gap fill off and one state-root thread.
The tables below ran with the node's defaults on Linux (`--db.gap-fill auto`, parallel state roots), measured on 2026-11-12.
Setup: a GCP c3-highcpu-8 (8 vCPU, 16 GB) with the datadir on a 200 GB Hyperdisk Balanced data disk (ext4), storage layout v2, durable commits, outputs run back to back.
Each cell is the mean of two runs of 300 busy or 150 peak outputs. The two runs of every cell agreed within 5%, except the 40,000-IOPS busy pair's time per output (512 and 460 ms, explained below).

Mean time per consensus output and in its commit:

| Disk (Hyperdisk Balanced) | Busy: per output | Busy: commit | Peak: per output | Peak: commit |
| --- | --- | --- | --- | --- |
| 10,000 IOPS, 800 MiB/s | 964 ms | 680 ms | 2,463 ms | 1,310 ms |
| 20,000 IOPS, 300 MiB/s | 807 ms | 492 ms | 2,598 ms | 1,324 ms |
| 20,000 IOPS, 800 MiB/s | 629 ms | 339 ms | 1,964 ms | 782 ms |
| 40,000 IOPS, 800 MiB/s | 486 ms | 157 ms | 1,670 ms | 408 ms |

What the node sustained, outputs back to back:

| Disk (Hyperdisk Balanced) | Busy: outputs/s | Busy: transactions/s | Peak: outputs/s | Peak: transactions/s |
| --- | --- | --- | --- | --- |
| 10,000 IOPS, 800 MiB/s | 1.04 | 2,424 | 0.41 | 4,763 |
| 20,000 IOPS, 300 MiB/s | 1.24 | 2,896 | 0.38 | 4,516 |
| 20,000 IOPS, 800 MiB/s | 1.59 | 3,713 | 0.51 | 5,972 |
| 40,000 IOPS, 800 MiB/s | 2.06 | 4,820 | 0.60 | 7,023 |

For scale, the benchmark timestamps its outputs one second apart.

- Execution (55 to 65 ms busy, about 260 ms peak) and the state root (73 to 78 ms busy, 360 to 380 ms peak) were the same on every disk. The commit was what the disk changed, and each doubling of IOPS about halved it.
- Throughput matters as well as IOPS, most at peak. A peak output changes about 76,000 pages, about 300 MiB, so at 300 MiB/s its commit takes over a second whatever the IOPS. At 20,000 IOPS, 300 MiB/s instead of 800 made a busy output 28% slower and a peak one 32% slower.
- `auto` fills wider on a disk with fewer IOPS, writing more bytes to issue fewer writes. Per busy output it wrote about 442 MiB at 10,000 IOPS, 247 MiB at 20,000 and 122 MiB at 40,000 (mean widths 14.9, 7.4 and 1.9 pages); per peak output 917, 570 and 306 MiB (mean widths 8.2, 4.0 and 0.04). At 10,000 IOPS a peak commit wrote about 700 MiB/s, close to the disk's 800 MiB/s limit.
- `auto` chose width 0 (no fill) where no wider width cost less, and otherwise only while it measured the disk at the start of a run:
  - 40,000 IOPS: 147 of each peak run's 150 commits, where its probes at width 2 took about 510 ms against about 400 ms at 0. In one busy run, 127 of its 300 commits (outputs 0 to 129), at the cost of width 2 there (159 ms); the other busy run filled at widths 2 and 4 throughout. That pair's times per output differ, 512 and 460 ms, though their commits took 160 and 155 ms: writing the blocks took 186 against 147 ms, most of the difference in the first 150 outputs, while the page cache was still filling.
  - 20,000 IOPS, 800 MiB/s: 4 and 16 of each busy run's 300 commits, all in the first 32 outputs, which averaged 581 and 367 ms against about 340 ms for the rest. None at peak.
  - 20,000 IOPS, 300 MiB/s: 23 and 19 of each busy run's commits, all in the first 31 outputs, which cost less than the rest (339 and 363 ms against 502); 8 of each peak run's 150 (its first 7 and one later probe), at about 1.57 s against 1.31 s.
  - 10,000 IOPS: none.

What this means for a validator:

- **Local NVMe is best.** It was the fastest disk measured with gap fill off (above), and it is the recommended disk in [Hardware Requirements](hardware-requirements.md#summary).
- **On cloud block storage, the minimum is 20,000 sustained write IOPS.** With the defaults, at 800 MiB/s, a busy output took 629 ms (1.59 outputs a second) and a peak one 1,964 ms (0.51 a second).
- **10,000 IOPS is not enough.** A busy output took 964 ms, about the second between the benchmark's outputs, so the node kept up with busy load with no margin, and at peak its commits wrote about 700 MiB/s of the disk's 800.
- **For cloud block storage, 40,000 IOPS is recommended.** Its commit took 157 ms of a 486 ms busy output (2.06 outputs a second) and 408 ms of a 1,670 ms peak one (0.60 a second), so more IOPS can save at most that commit time.
- **Provision at least 800 MiB/s with the IOPS.** At 20,000 IOPS, 300 MiB/s gave a busy output of 807 ms and a peak one of 2,598 ms, against 629 and 1,964 ms at 800 MiB/s: 28% and 32% slower. Nothing above 800 MiB/s was measured on cloud block storage.
- **No disk measured keeps up with peak load at one output a second.** Even at 40,000 IOPS a peak output took 1.7 s, 1.3 s of it outside the commit (execution, the state root and writing the blocks). Peak-sized outputs arriving that fast queue on any of these disks.
- **4,500 IOPS is too slow for a busy validator.** The commit of one peak output took 12 s or more.
- Quote provisioned, sustained figures, not burst figures.

An observer executes the same outputs and makes the same commits, so the same disk guidance applies.

## Gap fill (`--db.gap-fill`)

Gap fill trades bytes for I/Os.
Before a commit's sync, the node rewrites each short gap of unchanged pages between two runs of changed pages with the bytes those pages already hold.
The disk then gets fewer, larger writes.
The file's contents and size do not change; only more bytes are written.

| Value | Effect |
| --- | --- |
| `auto` | The default on Linux. Measures what the disk's syncs cost and picks a width for each commit, from 0 (no fill) up to 64 pages. |
| `off` or `0` | No gap fill. The default on macOS. |
| `1` to `64` | A fixed width: every gap of up to this many pages is filled. |

Busy profile, layout v2, 300 outputs, transactions per second (ratio to `off`):

| Disk | VM | `off` | `8` | `auto` |
| --- | --- | --- | --- | --- |
| GCP Hyperdisk Balanced, 4,500 IOPS | c3-highmem-8, 64 GB | 484 | 1,020 (2.1x) | 1,380 (2.85x) |
| GCP Hyperdisk Balanced, 20,000 IOPS | c3-highcpu-8, 16 GB | 2,150 | 3,515 (1.63x) | 3,157 (1.47x) |
| Local NVMe SSD, ext4 | c3-standard-8-lssd, 32 GB | 4,022 | 2,287 (0.57x) | 4,046 (1.00x) |

On local NVMe, `auto` settled at a mean width of 0.03, so it almost never filled.
On the 20,000-IOPS disk it settled at about 6 to 7.

When to override the default:

- **Keep `auto`** unless you have measured your own disk.
- **A fixed width of 8** beat `auto` on the 20,000-IOPS disk in the table above (1.63x against 1.47x). In four further busy runs on the 20,000-IOPS, 800 MiB/s disk of the per-IOPS setup, alternating a fixed 8 and `auto`, `auto` held width 8 for most commits and its commits took as long (342 against 341 ms); a fixed 8 was 3% faster per output (619 against 639 ms), all of it in the first 30 outputs, while `auto` was still measuring the disk. Use a fixed width only on a cloud disk you know caps IOPS and has throughput to spare, since it writes more bytes: on local NVMe, a width of 8 wrote about 3 times the bytes per commit that gap fill off wrote.
- **Never set a fixed width on local NVMe.** A width of 8 cut throughput to 0.57x there.
- **`off`** rules gap fill out while you look into a slow node. On local NVMe it changes nothing measurable, because `auto` already stays at 0 there.
- On a file system that refuses non-blocking reads, such as tmpfs, gap fill turns itself off and logs a warning once per open.

## Memory

Every node keeps a cross-block execution cache of recent accounts, storage slots and contract code, which block execution reads before the database.
It takes about 160 MiB, a fixed size with no setting.

- It is sized for execution, not for RPC. RPC reads do not use it, so an RPC node's reads depend on the operating system's page cache, which is free RAM.
- On the peak profile with layout v2, it served 81% of account reads and 82% of storage reads. A 2.5 GiB cache served 84% and 85%.
- On a Mac M1 Max (peak profile, layout v2) it cut execution from 186.6 ms to 138.7 ms per output.
- 160 MiB is small next to the validator minimum of 32 GB.
- `tn_reth.exec_cache_stale_hits_total` counts any value the cache served that a check found stale. The node checks one hit of each kind (account, slot, code) in 256 against the database. The count should stay 0.
- On a stale value the node drops the cache and builds that block again without it, so that block is right and the node keeps running. The unchecked values it served before may already have put the node on another state than its peers. Compare the node's recent block hashes with a peer's before you trust it, and report it as a bug.
- Each node checks its own sample of hits, so a 0 on other nodes does not clear one that counted a stale value.

## Linux and macOS

Linux is the production platform.

- `--db.gap-fill` defaults to `auto` on Linux.
- A file just written by `cp`, `rsync` or a restore sits in large page-cache folios. Until they are evicted, each commit writes whole folios back to disk, several times the bytes it changed. At its first open after a copy (when `db/tn-lmdb.dat.boot` is missing or names another file), the node drops `db/tn-lmdb.dat` from the page cache itself. Dropping the cache by hand before the first start (`sync; echo 1 | sudo tee /proc/sys/vm/drop_caches`) is harmless.

macOS is for development and testing, not for validators.

- `--db.gap-fill` defaults to `off` on macOS. macOS syncs a commit as ranged `msync` calls, which mislead `auto`'s cost model. No gap fill setting has been measured to speed up commits on macOS.
- macOS's `msync` and `fsync` do not flush the drive's write cache, and the node does not use `F_FULLFSYNC`. A process or kernel crash loses no committed data, but a power loss can.
