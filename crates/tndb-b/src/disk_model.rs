//! Test-only model of what a power loss keeps: for each tracked file (by inode, so a rename keeps
//! it), an image of the bytes that reached the disk, whose length is the file's *durable length*.
//! Every real flush updates it: an msync copies the OS pages it covers (whole pages, as the kernel
//! flushes them) but only within the durable length, and an fsync or fdatasync takes the whole
//! file, its length included. Growing or truncating the file changes only its live length: as
//! POSIX allows, a size change no fsync made durable can be lost, and with it every byte past the
//! durable length, msync'd or not. [`power_loss`] gives the file back its durable length and
//! writes the image over it: bytes never flushed are gone. [`power_loss_random`] models a harsher
//! crash in which some unflushed pages (and perhaps the size change) had already been written back,
//! some pages only in part. Untracked files cost one atomic load per flush.

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    os::unix::fs::{FileExt as _, MetadataExt as _},
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex, PoisonError,
    },
};

/// The unit a disk writes atomically in [`power_loss_random`]'s model.
const SECTOR: usize = 512;

static TRACKED: AtomicUsize = AtomicUsize::new(0);
static IMAGES: Mutex<Option<HashMap<u64, Vec<u8>>>> = Mutex::new(None);

fn images() -> std::sync::MutexGuard<'static, Option<HashMap<u64, Vec<u8>>>> {
    IMAGES.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Start modeling the file at `path`: its current bytes are taken as on disk.
pub(crate) fn track(path: &Path) {
    let ino = std::fs::metadata(path).expect("metadata").ino();
    let bytes = std::fs::read(path).expect("read the tracked file");
    images().get_or_insert_with(HashMap::new).insert(ino, bytes);
    TRACKED.fetch_add(1, Ordering::Relaxed);
}

/// Copy the tracked file at `from` to `to` and model the copy too, with the same image: the
/// copy's disk holds what the original's does, and its page cache what the original's does.
pub(crate) fn copy_tracked(from: &Path, to: &Path) {
    std::fs::copy(from, to).expect("copy the tracked file");
    let (_, image, _) = image_of(from);
    let ino = std::fs::metadata(to).expect("metadata").ino();
    images().get_or_insert_with(HashMap::new).insert(ino, image);
    TRACKED.fetch_add(1, Ordering::Relaxed);
}

/// Stop modeling the file at `path`.
pub(crate) fn untrack(path: &Path) {
    let ino = std::fs::metadata(path).expect("metadata").ino();
    if images().as_mut().and_then(|images| images.remove(&ino)).is_some() {
        TRACKED.fetch_sub(1, Ordering::Relaxed);
    }
}

/// An msync of `[start, end)` of the file with inode `ino`, mapped at `[base, base + mapped)`.
/// The pages are copied through the raw pointer, never borrowed as a slice, since readers may hold
/// slices of other bytes of the mapping meanwhile.
pub(crate) fn flushed_range(ino: u64, base: *const u8, mapped: u64, start: u64, end: u64) {
    if TRACKED.load(Ordering::Relaxed) == 0 || start >= end {
        return;
    }
    let mut images = images();
    let Some(image) = images.as_mut().and_then(|images| images.get_mut(&ino)) else {
        return;
    };
    let page = crate::sys::os_page_size();
    let from = (start - start % page) as usize;
    // Past the durable length the bytes may reach their blocks, but the size that makes them part
    // of the file may not: they stay lost until an fsync makes the length durable.
    let to = ((end.div_ceil(page) * page).min(mapped) as usize).min(image.len());
    if from >= to {
        return;
    }
    // SAFETY: the caller's `[base, base + mapped)` is a live mapping and `[from, to)` lies within
    // it; the copy goes into the model's own buffer, which no one else touches.
    unsafe {
        std::ptr::copy_nonoverlapping(base.add(from), image[from..to].as_mut_ptr(), to - from);
    }
}

/// The inode of `file`, if any file is tracked (else 0, never a tracked one).
pub(crate) fn ino_if_tracking(file: &File) -> u64 {
    if TRACKED.load(Ordering::Relaxed) == 0 {
        return 0;
    }
    file.metadata().map(|meta| meta.ino()).unwrap_or(0)
}

/// An fsync (or fdatasync) of `file`: every byte of it is on disk, and so is its length.
pub(crate) fn flushed_all(file: &File) {
    let ino = ino_if_tracking(file);
    if ino == 0 {
        return;
    }
    let mut images = images();
    let Some(image) = images.as_mut().and_then(|images| images.get_mut(&ino)) else {
        return;
    };
    let len = file.metadata().map(|meta| meta.len()).unwrap_or(0) as usize;
    let mut bytes = vec![0_u8; len];
    if file.read_exact_at(&mut bytes, 0).is_ok() {
        *image = bytes;
    }
}

/// The image of the tracked file at `path`, its inode, and the file opened for writing.
fn image_of(path: &Path) -> (u64, Vec<u8>, File) {
    let ino = std::fs::metadata(path).expect("metadata").ino();
    let image = images()
        .as_ref()
        .and_then(|images| images.get(&ino).cloned())
        .expect("the file is tracked");
    let file = OpenOptions::new().read(true).write(true).open(path).expect("open");
    (ino, image, file)
}

/// A power loss: the file at `path` keeps only what its image holds. It gets its durable length
/// back (an unsynced growth is cut off, an unsynced truncation undone) and every byte is the
/// image's.
pub(crate) fn power_loss(path: &Path) {
    let (_, image, file) = image_of(path);
    file.set_len(image.len() as u64).expect("restore the durable length");
    file.write_all_at(&image, 0).expect("write the image");
}

/// A power loss in which the kernel had already written back some unflushed pages, some of them
/// only in part. If the file's length differs from its durable length, a first seeded draw picks
/// which survives (the size change was written back early, or not); with equal lengths no draw
/// is spent. Each byte up to the surviving length has an *old* value, the image's (zero past the
/// durable length), and a *new* value, the file's current byte (the old one past the current
/// length). For every OS page whose old and new bytes differ, one seeded draw picks, with equal
/// odds:
/// - **old**: the whole page reads as the image (never written back, [`power_loss`]'s outcome);
/// - **new**: the whole page reads as the current bytes (written back early);
/// - **torn**: each 512-byte sector independently (a fair seeded coin) reads old or new: a
///   writeback the power loss cut short. A sector is never split: disks write sectors atomically.
///
/// Pages whose bytes all agree are left alone and cost no draw, so the outcome depends only on
/// `seed` and on which pages changed. The image becomes the resulting bytes (they are what the
/// disk now holds, length included), so a later [`power_loss`] keeps them.
pub(crate) fn power_loss_random(path: &Path, seed: u64) {
    let (ino, image, file) = image_of(path);
    let live = file.metadata().expect("metadata").len() as usize;
    let mut rng = SplitMix64(seed);
    let len = if live == image.len() || rng.next().is_multiple_of(2) { live } else { image.len() };
    // The old bytes, zero past the durable length; the new ones, old past the current length.
    let mut old = image;
    old.resize(len, 0);
    let mut bytes = vec![0_u8; live];
    file.read_exact_at(&mut bytes, 0).expect("read the file");
    bytes.truncate(len);
    bytes.extend_from_slice(&old[bytes.len()..]);
    let page = crate::sys::os_page_size() as usize;
    for start in (0..len).step_by(page) {
        let end = (start + page).min(len);
        if bytes[start..end] == old[start..end] {
            continue;
        }
        let outcome = rng.next() % 3;
        for sector in (start..end).step_by(SECTOR) {
            let to_old = match outcome {
                0 => true,
                1 => false,
                _ => rng.next() & 1 == 0,
            };
            if to_old {
                let range = sector..(sector + SECTOR).min(end);
                bytes[range.clone()].copy_from_slice(&old[range]);
            }
        }
    }
    file.set_len(len as u64).expect("set the surviving length");
    file.write_all_at(&bytes, 0).expect("write the surviving bytes");
    if let Some(image) = images().as_mut().and_then(|images| images.get_mut(&ino)) {
        *image = bytes;
    }
}

/// SplitMix64: a tiny seeded generator, stable across platforms and dependency versions.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        file::PageFile,
        sys::{fsync_file, os_page_size},
    };

    /// A tracked file of `pages` OS pages, every byte `fill`, taken as on disk.
    fn tracked_file(tmp: &tempfile::TempDir, pages: usize, fill: u8) -> (std::path::PathBuf, File) {
        let path = tmp.path().join("db");
        std::fs::write(&path, vec![fill; pages * os_page_size() as usize]).expect("write");
        track(&path);
        let file = OpenOptions::new().read(true).write(true).open(&path).expect("open");
        (path, file)
    }

    /// A strict power loss restores the last flushed image: an fsync'd write survives, a later
    /// unflushed one is gone, and an unflushed extension is cut off (the durable length comes
    /// back).
    #[test]
    fn power_loss_restores_the_flushed_image() {
        let tmp = tempfile::TempDir::with_prefix("tndb_b_power_loss").expect("temp dir");
        let page = os_page_size() as usize;
        let (path, file) = tracked_file(&tmp, 2, 0xAA);
        file.write_all_at(&vec![0xBB; page], 0).expect("write");
        fsync_file(&file).expect("fsync");
        file.write_all_at(&vec![0xCC; page], 0).expect("unflushed overwrite");
        file.write_all_at(&vec![0xDD; page], 2 * page as u64).expect("unflushed extension");
        power_loss(&path);
        let bytes = std::fs::read(&path).expect("read");
        assert_eq!(bytes.len(), 2 * page, "the unsynced extension is cut off");
        assert!(bytes[..page].iter().all(|&b| b == 0xBB), "the fsync'd write survives");
        assert!(bytes[page..].iter().all(|&b| b == 0xAA), "the untouched page stays");
        untrack(&path);
    }

    /// A size change is durable only once an fsync makes it so: a page msync'd past the durable
    /// length of a grown file is lost with the growth at a strict power loss, survives once an
    /// fdatasync made the length durable, and a random power loss either keeps the size change
    /// or cuts it off, leaving the durable bytes alone.
    #[test]
    fn a_size_change_is_durable_only_once_synced() {
        let tmp = tempfile::TempDir::with_prefix("tndb_b_power_loss_growth").expect("temp dir");
        let page = os_page_size() as usize;
        let (path, file) = tracked_file(&tmp, 2, 0xAA);
        drop(file);
        // Grow the file, write page `p` past the old end with `fill`, msync it and, with `sync`,
        // make the size durable; return the grown length.
        let grow_and_write = |p: u32, fill: u8, sync: bool| {
            let mut file = PageFile::open(&path, 1 << 26).expect("open");
            file.ensure_len((u64::from(p) + 1) * page as u64).expect("grow");
            file.page_mut(p, page).expect("page").fill(fill);
            let start = u64::from(p) * page as u64;
            let range = start..start + page as u64;
            file.msync_ranges(std::slice::from_ref(&range)).expect("msync");
            if sync {
                file.sync_size_if_grown().expect("size sync");
            }
            file.file_len()
        };
        grow_and_write(3, 0xBB, false);
        power_loss(&path);
        let bytes = std::fs::read(&path).expect("read");
        assert_eq!(bytes.len(), 2 * page, "the msync'd page past the durable length is lost");
        assert!(bytes.iter().all(|&b| b == 0xAA));

        let synced = grow_and_write(3, 0xBB, true) as usize;
        power_loss(&path);
        let bytes = std::fs::read(&path).expect("read");
        assert_eq!(bytes.len(), synced, "the synced size survives");
        assert!(bytes[3 * page..4 * page].iter().all(|&b| b == 0xBB), "and the page with it");

        let target = (synced / page) as u32 + 1;
        let grown = grow_and_write(target, 0xCC, false) as usize;
        let before = std::fs::read(&path).expect("read");
        let mut lengths = std::collections::BTreeSet::new();
        for seed in 0..16 {
            let copy = tmp.path().join(format!("copy{seed}"));
            copy_tracked(&path, &copy);
            power_loss_random(&copy, seed);
            let bytes = std::fs::read(&copy).expect("read");
            assert!(bytes.len() == synced || bytes.len() == grown, "{} bytes", bytes.len());
            assert_eq!(bytes[..synced], before[..synced], "the durable bytes are left alone");
            lengths.insert(bytes.len());
            untrack(&copy);
        }
        assert_eq!(lengths.len(), 2, "the size change is sometimes kept, sometimes lost");
        untrack(&path);
    }

    /// Run [`power_loss_random`] with `seed` over a fresh file of `lead + 64` OS pages of 0xAA (on
    /// disk) whose last 64 pages are overwritten with 0xBB (not flushed), except the sixth of them,
    /// which is left alone; return the bytes of those 64 pages.
    fn random_loss(seed: u64, lead: usize) -> Vec<u8> {
        let tmp = tempfile::TempDir::with_prefix("tndb_b_power_loss_random").expect("temp dir");
        let page = os_page_size() as usize;
        let (path, file) = tracked_file(&tmp, lead + 64, 0xAA);
        for p in (0..64).filter(|&p| p != 5) {
            file.write_all_at(&vec![0xBB; page], ((lead + p) * page) as u64).expect("write");
        }
        power_loss_random(&path, seed);
        let bytes = std::fs::read(&path).expect("read");
        assert!(bytes[..lead * page].iter().all(|&b| b == 0xAA), "unchanged pages are left alone");
        // The image is now the surviving bytes: a strict power loss keeps them.
        power_loss(&path);
        assert_eq!(std::fs::read(&path).expect("read"), bytes, "the image is what survived");
        untrack(&path);
        bytes[lead * page..].to_vec()
    }

    /// A random power loss mixes old and new bytes per page and per 512-byte sector, never inside a
    /// sector, leaves unchanged pages alone (spending no draw on them), and is reproducible from
    /// its seed.
    #[test]
    fn power_loss_random_mixes_sectors_reproducibly() {
        let page = os_page_size() as usize;
        let bytes = random_loss(7, 0);
        assert_eq!(bytes.len(), 64 * page, "the file keeps its length");
        let sectors: Vec<u8> = bytes
            .chunks(512)
            .map(|sector| {
                assert!(sector.iter().all(|&b| b == sector[0]), "a sector is never split");
                sector[0]
            })
            .collect();
        let per_page = page / 512;
        let pages: Vec<&[u8]> = sectors.chunks(per_page).collect();
        assert!(pages[5].iter().all(|&b| b == 0xAA), "an unchanged page is left alone");
        let all = |p: &[u8], b: u8| p.iter().all(|&s| s == b);
        assert!(pages.iter().any(|p| all(p, 0xAA)), "some pages keep the flushed image");
        assert!(pages.iter().any(|p| all(p, 0xBB)), "some pages were written back early");
        assert!(
            pages.iter().any(|p| p.contains(&0xAA) && p.contains(&0xBB)),
            "some pages are torn between old and new sectors"
        );
        assert_eq!(random_loss(7, 0), bytes, "the same seed gives the same outcome");
        assert_eq!(random_loss(7, 3), bytes, "unchanged pages cost no draw");
        assert_ne!(random_loss(8, 0), bytes, "another seed gives another outcome");
    }
}
