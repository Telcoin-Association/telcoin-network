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
Each cell is the mean of two runs of 300 busy or 150 peak outputs, and the two runs of every cell agreed within 4%.

Mean time per consensus output and in its commit:

| Disk (Hyperdisk Balanced) | Busy: per output | Busy: commit | Peak: per output | Peak: commit |
| --- | --- | --- | --- | --- |
| 10,000 IOPS, 800 MiB/s | 991 ms | 714 ms | 2,566 ms | 1,401 ms |
| 20,000 IOPS, 300 MiB/s | 876 ms | 560 ms | 2,764 ms | 1,477 ms |
| 20,000 IOPS, 800 MiB/s | 703 ms | 405 ms | 1,959 ms | 773 ms |
| 40,000 IOPS, 800 MiB/s | 543 ms | 199 ms | 1,706 ms | 407 ms |

What the node sustained, outputs back to back:

| Disk (Hyperdisk Balanced) | Busy: outputs/s | Busy: transactions/s | Peak: outputs/s | Peak: transactions/s |
| --- | --- | --- | --- | --- |
| 10,000 IOPS, 800 MiB/s | 1.01 | 2,359 | 0.39 | 4,573 |
| 20,000 IOPS, 300 MiB/s | 1.14 | 2,669 | 0.36 | 4,245 |
| 20,000 IOPS, 800 MiB/s | 1.42 | 3,326 | 0.51 | 5,990 |
| 40,000 IOPS, 800 MiB/s | 1.84 | 4,308 | 0.59 | 6,875 |

For scale, the benchmark timestamps its outputs one second apart.

- Execution (55 to 65 ms busy, about 260 ms peak) and the state root (about 77 ms busy, 376 ms peak) were the same on every disk. The commit was what the disk changed, and each doubling of IOPS about halved it.
- Throughput matters as well as IOPS, most at peak. A peak output changes about 76,000 pages, about 300 MiB, so at 300 MiB/s its commit takes over a second whatever the IOPS. At 20,000 IOPS, 300 MiB/s instead of 800 made a busy output 25% slower and a peak one 41% slower.
- `auto` fills wider on a disk with fewer IOPS, writing more bytes to issue fewer writes. Per busy output it wrote about 460 MiB at 10,000 IOPS, 235 MiB at 20,000 and 118 MiB at 40,000 (mean widths 15.8, 7.2 and 1.5 pages); per peak output 967, 571 and 307 MiB. At 10,000 IOPS the peak runs wrote at the disk's 800 MiB/s limit (95th percentile of their seconds).
- `auto` chose width 0 (no fill) for some commits on every disk. On the 40,000-IOPS disk that was its usual choice and the faster one. Elsewhere it was slower, and those commits raised the means above:
  - 20,000 IOPS, 800 MiB/s, busy: 36 and 53 of each run's 300 commits, 18 or 19 of them in the first 30 outputs and the rest in short runs of consecutive commits later on. They took 500 to 560 ms against about 380 ms for the rest; the other outputs averaged 657 ms against 703 ms for all. The peak runs had 0 and 2 such commits.
  - 10,000 IOPS, busy: 11 and 4 commits, all in the first 30 outputs, of about 1.5 s against 0.69 s; the other outputs averaged 964 ms against 991 ms. The peak runs had 1 and 0.
  - 20,000 IOPS, 300 MiB/s, peak: 67 and 52 of each run's 150 commits, most of them in the first 55 outputs, at about 1.67 s against 1.35 s; the other outputs averaged about 2,573 ms against 2,764 ms. Its busy runs' width-0 commits were no slower than the rest.

What this means for a validator:

- **Local NVMe is best.** It was the fastest disk measured with gap fill off (above), and it is the recommended disk in [Hardware Requirements](hardware-requirements.md#summary).
- **On cloud block storage, the minimum is 20,000 sustained write IOPS.** With the defaults, at 800 MiB/s, a busy output took 703 ms (1.42 outputs a second) and a peak one 1,959 ms (0.51 a second).
- **10,000 IOPS is not enough.** A busy output took 991 ms, about the second between the benchmark's outputs, so the node kept up with busy load with no margin, and at peak its commits used the disk's whole 800 MiB/s.
- **For cloud block storage, 40,000 IOPS is recommended.** Its commit took 199 ms of a 543 ms busy output (1.84 outputs a second) and 407 ms of a 1,706 ms peak one (0.59 a second), so more IOPS can save at most that commit time.
- **Provision throughput with the IOPS.** At 20,000 IOPS, 300 MiB/s gave a busy output of 876 ms and a peak one of 2,764 ms, against 703 and 1,959 ms at 800 MiB/s.
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
- **A fixed width of 8** beat `auto` on the 20,000-IOPS disk (1.63x against 1.47x). Use it only on a cloud disk you know caps IOPS and has throughput to spare, since it writes more bytes: on local NVMe, a width of 8 wrote about 3 times the bytes per commit that gap fill off wrote.
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
- `tn_reth.exec_cache_stale_hits_total` counts any value the cache served that a check found stale. It should stay 0. Report anything else as a bug.

## Linux and macOS

Linux is the production platform.

- `--db.gap-fill` defaults to `auto` on Linux.
- A file just written by `cp`, `rsync` or a restore sits in large page-cache folios. Until they are evicted, each commit writes whole folios back to disk, several times the bytes it changed. At its first open after a copy (when `db/tn-lmdb.dat.boot` is missing or names another file), the node drops `db/tn-lmdb.dat` from the page cache itself. Dropping the cache by hand before the first start (`sync; echo 1 | sudo tee /proc/sys/vm/drop_caches`) is harmless.

macOS is for development and testing, not for validators.

- `--db.gap-fill` defaults to `off` on macOS. macOS syncs a commit as ranged `msync` calls, which mislead `auto`'s cost model. No gap fill setting has been measured to speed up commits on macOS.
- macOS's `msync` and `fsync` do not flush the drive's write cache, and the node does not use `F_FULLFSYNC`. A process or kernel crash loses no committed data, but a power loss can.
