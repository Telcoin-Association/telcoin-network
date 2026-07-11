//! Object-store construction and access from a snapshot URL.
//!
//! [`SnapshotStore`] wraps an [`object_store::ObjectStore`] behind a single, uniform key layout so
//! the rest of the snapshot feature can upload, download, verify, and prune snapshot artifacts
//! without caring which backend (S3, GCS, Azure, HTTP, or local file) is in use. It owns:
//!
//! - **construction from a URL** ([`SnapshotStore::open`]) with per-scheme credentials read from
//!   the process environment,
//! - the **bucket key layout** ([`SnapshotStore::epoch_dir`] and friends) — the canonical source of
//!   truth for where each object lives,
//! - **streaming, integrity-checked transfers** ([`SnapshotStore::put_file`] /
//!   [`SnapshotStore::get_verified`]) that hash with sha256 while they stream, so multi-hundred-
//!   megabyte artifacts never have to sit whole in memory, and
//! - **zstd compression helpers** with an anti-bomb cap on decompressed size.
//!
//! The two compression helpers are synchronous std-io functions; callers on an async runtime should
//! wrap them in [`tokio::task::spawn_blocking`]. Everything else here is async.

use crate::{SnapshotError, SnapshotResult};
use futures::StreamExt;
use object_store::{
    aws::AmazonS3Builder, azure::MicrosoftAzureBuilder, gcp::GoogleCloudStorageBuilder,
    http::HttpBuilder, local::LocalFileSystem, path::Path, ObjectStore, ObjectStoreExt,
    WriteMultipart,
};
use sha2::{Digest, Sha256};
use std::{
    fmt::Write as _,
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path as LocalPath,
    sync::Arc,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use url::{Position, Url};

/// Prefix of every per-epoch "directory" key, e.g. `epoch-0000000042`.
const EPOCH_DIR_PREFIX: &str = "epoch-";

/// Filename of the manifest object inside each epoch directory.
const MANIFEST_FILENAME: &str = "manifest.json";

/// Multipart part size used when streaming a file into a multipart upload.
///
/// Chosen to keep well under S3's 10,000-part ceiling (32 MiB × 10,000 ≈ 320 GiB per object) while
/// bounding peak memory to roughly `MULTIPART_PART_SIZE * MAX_UPLOAD_CONCURRENCY`.
const MULTIPART_PART_SIZE: usize = 32 * 1024 * 1024;

/// Maximum number of in-flight upload parts before the producer blocks.
///
/// Applied as backpressure before buffering each chunk so a fast local disk feeding a slow network
/// cannot spawn unbounded upload tasks.
const MAX_UPLOAD_CONCURRENCY: usize = 4;

/// Granularity at which a local file is read while being streamed into a multipart upload.
const UPLOAD_READ_CHUNK: usize = 8 * 1024 * 1024;

/// Read buffer size (and cap-check granularity) used while streaming a zstd decode.
const DECOMPRESS_BUF_SIZE: usize = 256 * 1024;

/// zstd compression level; a fast default suited to write-once snapshot artifacts.
const ZSTD_LEVEL: i32 = 3;

/// Handle to the remote (or local) object store holding a node's snapshots.
///
/// A `SnapshotStore` pairs an [`ObjectStore`] backend with a key `prefix` parsed from the snapshot
/// URL. Every method takes keys **relative** to that prefix and resolves them through
/// `SnapshotStore::location`, so callers never deal with bucket names or the base path directly.
///
/// Construct one from a URL with [`SnapshotStore::open`]; tests and advanced callers may inject a
/// preconfigured backend with [`SnapshotStore::with_store`].
#[derive(Clone, Debug)]
pub struct SnapshotStore {
    /// the backend all I/O is dispatched to.
    store: Arc<dyn ObjectStore>,
    /// key prefix parsed from the URL path; every key is resolved relative to this.
    prefix: Path,
}

impl SnapshotStore {
    /// Object key of the `latest.json` pointer, relative to the store prefix.
    ///
    /// This is the canonical key helper for the pointer document. The `manifest` module defines a
    /// same-valued filename constant for its own use; keeping a one-word duplicate here avoids a
    /// cross-module import while this module compiles independently.
    pub const LATEST_KEY: &'static str = "latest.json";

    /// Open a snapshot store from a URL, reading credentials from the process environment.
    ///
    /// Supported schemes and their credential sources:
    ///
    /// - `s3://bucket/prefix` (also `s3a`) — [`AmazonS3Builder::from_env`] reads the standard
    ///   `AWS_*` variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`,
    ///   `AWS_SESSION_TOKEN`, `AWS_ENDPOINT`, …); the bucket comes from the URL host.
    /// - `gs://bucket/prefix` — [`GoogleCloudStorageBuilder::from_env`] reads
    ///   `GOOGLE_SERVICE_ACCOUNT`, `GOOGLE_SERVICE_ACCOUNT_PATH`, `GOOGLE_SERVICE_ACCOUNT_KEY`, or
    ///   `GOOGLE_APPLICATION_CREDENTIALS`.
    /// - `azure://container/prefix` (also `az`, `abfs`, `abfss`, `adl`) —
    ///   [`MicrosoftAzureBuilder::from_env`] reads `AZURE_STORAGE_ACCOUNT_NAME` together with an
    ///   `AZURE_STORAGE_ACCOUNT_KEY` / `AZURE_STORAGE_ACCESS_KEY` / `AZURE_STORAGE_SAS_KEY`.
    /// - `http(s)://host/path` — an [`HttpBuilder`] with no credentials; suited to public,
    ///   pre-signed, or reverse-proxied endpoints (any `user:pass@` in the URL is honored).
    /// - `file:///abs/path` — a [`LocalFileSystem`]; the directory is created if absent, making `file://`
    ///   the credential-free path for tests and local end-to-end runs.
    ///
    /// The explicit `from_env` builders are used deliberately: object_store's [`parse_url_opts`]
    /// does **not** read per-scheme credential environment variables, so relying on it would
    /// silently produce an unauthenticated client.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::InvalidUrl`] if `url` does not parse,
    /// [`SnapshotError::UnsupportedScheme`] for any other scheme, and
    /// [`SnapshotError::ObjectStore`] / [`SnapshotError::Io`] if the backend cannot be built or the
    /// local directory cannot be created.
    ///
    /// [`parse_url_opts`]: object_store::parse_url_opts
    pub fn open(url: &str) -> SnapshotResult<Self> {
        let parsed = Url::parse(url)?;

        let store: Arc<dyn ObjectStore> = match parsed.scheme() {
            "s3" | "s3a" => Arc::new(AmazonS3Builder::from_env().with_url(url).build()?),
            "gs" => Arc::new(GoogleCloudStorageBuilder::from_env().with_url(url).build()?),
            "az" | "azure" | "abfs" | "abfss" | "adl" => {
                Arc::new(MicrosoftAzureBuilder::from_env().with_url(url).build()?)
            }
            "http" | "https" => {
                // the http store is rooted at the origin; the url path becomes the key prefix
                let base = &parsed[..Position::BeforePath];
                Arc::new(HttpBuilder::new().with_url(base).build()?)
            }
            "file" => {
                let dir = parsed.to_file_path().map_err(|()| {
                    SnapshotError::Other(eyre::eyre!(
                        "file url must be an absolute path with an empty host: {url}"
                    ))
                })?;
                std::fs::create_dir_all(&dir)?;
                Arc::new(LocalFileSystem::new())
            }
            other => return Err(SnapshotError::UnsupportedScheme(other.to_string())),
        };

        // the url path is the key prefix for every scheme: for cloud the bucket/container is the
        // host (not the path), for local the store roots at `/` so the absolute path is the prefix.
        let prefix = Path::from_url_path(parsed.path()).map_err(object_store::Error::from)?;

        Ok(Self { store, prefix })
    }

    /// Wrap a preconstructed backend with an empty key prefix.
    ///
    /// Intended for tests (against `object_store::memory::InMemory`) and for callers that build a
    /// custom [`ObjectStore`] themselves. Keys are resolved relative to the store root.
    pub fn with_store(store: Arc<dyn ObjectStore>) -> Self {
        Self { store, prefix: Path::default() }
    }

    /// Return the directory key for `epoch`, zero-padded to ten digits.
    ///
    /// A `u32` never exceeds ten decimal digits, so the fixed width makes lexical ordering of the
    /// resulting keys (e.g. from a bucket listing) match numeric epoch ordering.
    pub fn epoch_dir(epoch: u32) -> String {
        format!("{EPOCH_DIR_PREFIX}{epoch:010}")
    }

    /// Return the manifest object key for `epoch`, i.e. `epoch-…/manifest.json`.
    pub fn manifest_key(epoch: u32) -> String {
        format!("{}/{MANIFEST_FILENAME}", Self::epoch_dir(epoch))
    }

    /// Return the key of the artifact named `name` inside `epoch`'s directory.
    pub fn artifact_key(epoch: u32, name: &str) -> String {
        format!("{}/{name}", Self::epoch_dir(epoch))
    }

    /// Parse an epoch directory name back into its epoch number.
    ///
    /// Returns `None` for any name that is not `epoch-<digits>` with a value that fits in a `u32`,
    /// so non-epoch keys in a listing are ignored rather than misread.
    pub fn parse_epoch_dir(name: &str) -> Option<u32> {
        let digits = name.strip_prefix(EPOCH_DIR_PREFIX)?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        digits.parse().ok()
    }

    /// Stream a local file to `key`, returning its `(size, sha256_hex)`.
    ///
    /// The file is uploaded with a multipart write and hashed with sha256 as it streams, so its
    /// contents are never buffered whole in memory. Backpressure bounds the number of in-flight
    /// parts. On any failure the multipart upload is aborted best-effort to limit orphaned parts;
    /// configuring a bucket lifecycle rule to reap incomplete uploads is still recommended, since
    /// an abort request can itself fail.
    ///
    /// The returned digest is lowercase hex.
    pub async fn put_file(
        &self,
        key: &str,
        local_path: &LocalPath,
    ) -> SnapshotResult<(u64, String)> {
        let location = self.location(key)?;
        let mut file = tokio::fs::File::open(local_path).await?;
        let upload = self.store.put_multipart(&location).await?;
        let mut writer = WriteMultipart::new_with_chunk_size(upload, MULTIPART_PART_SIZE);
        let mut hasher = Sha256::new();
        let mut size: u64 = 0;

        // stream the file into the upload, hashing every byte; the block borrows the writer so it
        // can still be moved into `finish`/`abort` afterwards.
        let streamed: SnapshotResult<()> = async {
            let mut buffer = vec![0u8; UPLOAD_READ_CHUNK];
            loop {
                let read = file.read(&mut buffer).await?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
                size += read as u64;
                // apply backpressure before buffering the next chunk
                writer.wait_for_capacity(MAX_UPLOAD_CONCURRENCY).await?;
                writer.write(&buffer[..read]);
            }
            Ok(())
        }
        .await;

        match streamed {
            Ok(()) => {
                // finish flushes the final part and completes the upload, aborting internally if
                // completion fails.
                writer.finish().await?;
                let digest = hasher.finalize();
                Ok((size, hex_lower(&digest)))
            }
            Err(err) => {
                let _ = writer.abort().await;
                Err(err)
            }
        }
    }

    /// Upload `bytes` to `key` in a single request. Intended for small documents.
    pub async fn put_bytes(&self, key: &str, bytes: Vec<u8>) -> SnapshotResult<()> {
        let location = self.location(key)?;
        self.store.put(&location, bytes.into()).await?;
        Ok(())
    }

    /// Download `key` to `dest`, enforcing both `expected_size` and `expected_sha256_hex`.
    ///
    /// The object is streamed to `dest` and hashed as it is written. The object's advertised size
    /// is checked before the destination file is created; the streamed byte count and sha256 digest
    /// are checked after. Any mismatch — or any transport error mid-stream — removes the partial
    /// destination file and returns [`SnapshotError::Integrity`] naming the key and the expected
    /// versus actual values. `expected_sha256_hex` is compared case-insensitively.
    pub async fn get_verified(
        &self,
        key: &str,
        expected_size: u64,
        expected_sha256_hex: &str,
        dest: &LocalPath,
    ) -> SnapshotResult<()> {
        let location = self.location(key)?;
        let result = self.store.get(&location).await?;

        // fail fast on the advertised size before creating any local file
        if result.meta.size != expected_size {
            return Err(SnapshotError::Integrity(format!(
                "size mismatch for {key}: expected {expected_size}, object advertises {}",
                result.meta.size
            )));
        }

        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // the block owns the destination file so it is closed before any cleanup removes it
        let downloaded: SnapshotResult<(u64, String)> = async {
            let mut file = tokio::fs::File::create(dest).await?;
            let mut hasher = Sha256::new();
            let mut size: u64 = 0;
            let mut stream = result.into_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                hasher.update(&chunk);
                size += chunk.len() as u64;
                file.write_all(&chunk).await?;
            }
            file.flush().await?;
            let digest = hasher.finalize();
            Ok((size, hex_lower(&digest)))
        }
        .await;

        match downloaded {
            Ok((size, actual)) => {
                if size != expected_size {
                    let _ = tokio::fs::remove_file(dest).await;
                    Err(SnapshotError::Integrity(format!(
                        "size mismatch for {key}: expected {expected_size}, downloaded {size}"
                    )))
                } else if !actual.eq_ignore_ascii_case(expected_sha256_hex) {
                    let _ = tokio::fs::remove_file(dest).await;
                    Err(SnapshotError::Integrity(format!(
                        "sha256 mismatch for {key}: expected {expected_sha256_hex}, got {actual}"
                    )))
                } else {
                    Ok(())
                }
            }
            Err(err) => {
                let _ = tokio::fs::remove_file(dest).await;
                Err(err)
            }
        }
    }

    /// Fetch a small JSON document, returning its raw bytes.
    ///
    /// Bytes (not a parsed value) are returned so a caller can sha256-check the binding between a
    /// pointer document and the manifest it references. Typed parsing and validation live in the
    /// `manifest` module.
    pub async fn get_json_bytes(&self, key: &str) -> SnapshotResult<Vec<u8>> {
        let location = self.location(key)?;
        let bytes = self.store.get(&location).await?.bytes().await?;
        Ok(bytes.to_vec())
    }

    /// Upload a small JSON document verbatim.
    ///
    /// This is byte-oriented on purpose: the `manifest` module owns typed serialization. When
    /// publishing a snapshot, the `latest.json` pointer written with this method **must be the
    /// final write** of the publish, so a reader never sees a pointer to a not-yet-complete
    /// epoch. That ordering is enforced by the uploader service, not here.
    pub async fn put_json_bytes(&self, key: &str, bytes: Vec<u8>) -> SnapshotResult<()> {
        self.put_bytes(key, bytes).await
    }

    /// List the epochs present in the store, sorted ascending.
    ///
    /// Backed by a delimited listing of the prefix, so it reads the epoch "directories" without
    /// enumerating their contents. Keys that are not `epoch-<digits>` directories (including the
    /// `latest.json` pointer) are ignored.
    pub async fn list_epochs(&self) -> SnapshotResult<Vec<u32>> {
        let listing = self.store.list_with_delimiter(self.prefix_opt()).await?;
        let mut epochs: Vec<u32> = listing
            .common_prefixes
            .iter()
            .filter_map(|prefix| prefix.filename().and_then(Self::parse_epoch_dir))
            .collect();
        epochs.sort_unstable();
        epochs.dedup();
        Ok(epochs)
    }

    /// Delete every object under `epoch`'s directory.
    ///
    /// Objects are listed first, then removed with a single bulk delete stream. Succeeds as a no-op
    /// if the directory is already absent.
    pub async fn delete_epoch_dir(&self, epoch: u32) -> SnapshotResult<()> {
        let dir = self.location(&Self::epoch_dir(epoch))?;

        let mut listing = self.store.list(Some(&dir));
        let mut locations = Vec::new();
        while let Some(meta) = listing.next().await {
            locations.push(meta?.location);
        }
        if locations.is_empty() {
            return Ok(());
        }

        let to_delete =
            futures::stream::iter(locations.into_iter().map(Ok::<Path, object_store::Error>))
                .boxed();
        let mut deletions = self.store.delete_stream(to_delete);
        while let Some(result) = deletions.next().await {
            result?;
        }
        Ok(())
    }

    /// Prune old epochs, keeping the newest `keep_last` and never deleting `protected`.
    ///
    /// Every epoch older than the newest `keep_last` is a deletion candidate, except `protected` —
    /// the epoch that `latest.json` currently points to — which is always retained even when it
    /// falls outside the kept window. Returns the epochs actually deleted, ascending.
    pub async fn prune(&self, keep_last: u32, protected: Option<u32>) -> SnapshotResult<Vec<u32>> {
        let mut epochs = self.list_epochs().await?;
        let keep = keep_last as usize;
        let mut deleted = Vec::new();

        if epochs.len() > keep {
            let remove_count = epochs.len() - keep;
            // candidates are the oldest `remove_count`, since `epochs` is sorted ascending
            let candidates: Vec<u32> = epochs.drain(..remove_count).collect();
            for epoch in candidates {
                if Some(epoch) == protected {
                    continue;
                }
                self.delete_epoch_dir(epoch).await?;
                deleted.push(epoch);
            }
        }

        Ok(deleted)
    }

    /// Compress `src` into `dst` with zstd, returning `(uncompressed, compressed)` byte counts.
    ///
    /// Synchronous std-io; wrap in [`tokio::task::spawn_blocking`] on an async runtime.
    pub fn compress_file(src: &LocalPath, dst: &LocalPath) -> SnapshotResult<(u64, u64)> {
        let mut input = BufReader::new(std::fs::File::open(src)?);
        let output = CountingWriter::new(std::fs::File::create(dst)?);
        let mut encoder = zstd::Encoder::new(output, ZSTD_LEVEL)?;
        let uncompressed = io::copy(&mut input, &mut encoder)?;
        let mut output = encoder.finish()?;
        output.flush()?;
        Ok((uncompressed, output.count))
    }

    /// Decompress `src` into `dst`, refusing to emit more than `max_decompressed` bytes.
    ///
    /// Decompressed bytes are counted as they are produced; exceeding the cap removes the partial
    /// output and returns [`SnapshotError::Integrity`]. This is the anti-bomb guard: a small,
    /// highly compressible input cannot expand without bound onto disk. Returns the decompressed
    /// size on success.
    ///
    /// Synchronous std-io; wrap in [`tokio::task::spawn_blocking`] on an async runtime.
    pub fn decompress_file_capped(
        src: &LocalPath,
        dst: &LocalPath,
        max_decompressed: u64,
    ) -> SnapshotResult<u64> {
        // the closure owns the output file so it is dropped (and closed) before any cleanup removes
        // the partial file below.
        let outcome = (|| -> SnapshotResult<u64> {
            let mut decoder = zstd::Decoder::new(BufReader::new(std::fs::File::open(src)?))?;
            let mut output = BufWriter::new(std::fs::File::create(dst)?);
            let mut buffer = vec![0u8; DECOMPRESS_BUF_SIZE];
            let mut total: u64 = 0;
            loop {
                let read = decoder.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                total += read as u64;
                if total > max_decompressed {
                    return Err(SnapshotError::Integrity(format!(
                        "decompressed size exceeds cap of {max_decompressed} bytes for {}",
                        src.display()
                    )));
                }
                output.write_all(&buffer[..read])?;
            }
            output.flush()?;
            Ok(total)
        })();

        match outcome {
            Ok(total) => Ok(total),
            Err(err) => {
                let _ = std::fs::remove_file(dst);
                Err(err)
            }
        }
    }

    /// Resolve a caller-supplied key into a full object [`Path`] under the store prefix.
    fn location(&self, key: &str) -> SnapshotResult<Path> {
        let parsed = Path::parse(key).map_err(object_store::Error::from)?;
        let mut location = self.prefix.clone();
        for part in parsed.parts() {
            // join escapes each segment individually; parsing `key` first keeps its `/`
            // separators as real path boundaries rather than encoding them into one segment.
            location = location.join(part);
        }
        Ok(location)
    }

    /// The prefix to pass to a listing call, or `None` when it is the store root.
    ///
    /// An empty prefix is normalized to `None` because some backends treat an empty prefix path
    /// differently from "list from the root".
    fn prefix_opt(&self) -> Option<&Path> {
        if self.prefix.parts().next().is_none() {
            None
        } else {
            Some(&self.prefix)
        }
    }
}

/// A [`Write`] adapter that tallies the total number of bytes written through it.
///
/// Used to measure a zstd-compressed stream's on-disk size without a second `stat` of the file.
struct CountingWriter<W> {
    inner: W,
    count: u64,
}

impl<W> CountingWriter<W> {
    fn new(inner: W) -> Self {
        Self { inner, count: 0 }
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.count += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Encode `bytes` as a lowercase hex string.
fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // writing to a string is infallible
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    /// Build a `file://` store rooted at `dir`.
    fn file_store(dir: &LocalPath) -> SnapshotStore {
        let url = Url::from_directory_path(dir).unwrap();
        SnapshotStore::open(url.as_str()).unwrap()
    }

    #[test]
    fn epoch_dir_is_zero_padded_and_orders_lexically() {
        assert_eq!(SnapshotStore::epoch_dir(42), "epoch-0000000042");
        assert_eq!(SnapshotStore::manifest_key(42), "epoch-0000000042/manifest.json");
        assert_eq!(
            SnapshotStore::artifact_key(42, "reth.tar.zst"),
            "epoch-0000000042/reth.tar.zst"
        );
        // fixed width => lexical order matches numeric order
        assert!(SnapshotStore::epoch_dir(2) < SnapshotStore::epoch_dir(10));
    }

    #[test]
    fn parse_epoch_dir_round_trips_and_rejects_junk() {
        assert_eq!(SnapshotStore::parse_epoch_dir("epoch-0000000042"), Some(42));
        assert_eq!(
            SnapshotStore::parse_epoch_dir(&SnapshotStore::epoch_dir(u32::MAX)),
            Some(u32::MAX)
        );
        assert_eq!(SnapshotStore::parse_epoch_dir("epoch-"), None);
        assert_eq!(SnapshotStore::parse_epoch_dir("epoch-12x"), None);
        assert_eq!(SnapshotStore::parse_epoch_dir("epoch-99999999999"), None); // overflows u32
        assert_eq!(SnapshotStore::parse_epoch_dir("nope"), None);
        assert_eq!(SnapshotStore::parse_epoch_dir("latest.json"), None);
    }

    #[test]
    fn open_rejects_unknown_scheme() {
        let err = SnapshotStore::open("ftp://example.com/snap").unwrap_err();
        assert!(matches!(err, SnapshotError::UnsupportedScheme(scheme) if scheme == "ftp"));
    }

    #[test]
    fn open_rejects_malformed_url() {
        let err = SnapshotStore::open("not a url").unwrap_err();
        assert!(matches!(err, SnapshotError::InvalidUrl(_)));
    }

    #[tokio::test]
    async fn put_file_get_verified_round_trip() {
        let remote = tempdir().unwrap();
        let store = file_store(remote.path());
        let staging = tempdir().unwrap();

        // ~10 MiB with varied content so the read loop iterates more than once
        let data: Vec<u8> = (0..10 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        let src = staging.path().join("artifact.bin");
        std::fs::write(&src, &data).unwrap();

        let key = SnapshotStore::artifact_key(5, "artifact.bin");
        let (size, hash) = store.put_file(&key, &src).await.unwrap();
        assert_eq!(size, data.len() as u64);

        let dest = staging.path().join("restored.bin");
        store.get_verified(&key, size, &hash, &dest).await.unwrap();
        assert_eq!(std::fs::read(&dest).unwrap(), data);
    }

    #[tokio::test]
    async fn get_verified_detects_corruption_and_removes_partial() {
        let remote = tempdir().unwrap();
        let store = file_store(remote.path());
        let staging = tempdir().unwrap();

        let data = vec![7u8; 3 * 1024 * 1024];
        let src = staging.path().join("a.bin");
        std::fs::write(&src, &data).unwrap();

        let key = SnapshotStore::artifact_key(1, "a.bin");
        let (size, hash) = store.put_file(&key, &src).await.unwrap();

        // flip one byte of the stored object without changing its size
        let stored = remote.path().join(&key);
        let mut bytes = std::fs::read(&stored).unwrap();
        bytes[0] ^= 0xFF;
        std::fs::write(&stored, &bytes).unwrap();

        let dest = staging.path().join("out.bin");
        let err = store.get_verified(&key, size, &hash, &dest).await.unwrap_err();
        assert!(matches!(err, SnapshotError::Integrity(_)));
        assert!(!dest.exists());
    }

    #[tokio::test]
    async fn get_verified_rejects_wrong_size() {
        let store = SnapshotStore::with_store(Arc::new(InMemory::new()));
        let data = vec![1u8, 2, 3, 4, 5];
        let key = SnapshotStore::artifact_key(1, "x");
        store.put_bytes(&key, data.clone()).await.unwrap();

        let staging = tempdir().unwrap();
        let dest = staging.path().join("out");
        let err = store
            .get_verified(&key, (data.len() + 1) as u64, &"00".repeat(32), &dest)
            .await
            .unwrap_err();
        assert!(matches!(err, SnapshotError::Integrity(_)));
        assert!(!dest.exists());
    }

    #[tokio::test]
    async fn json_bytes_round_trip() {
        let store = SnapshotStore::with_store(Arc::new(InMemory::new()));
        let doc = br#"{"latest":42}"#.to_vec();
        store.put_json_bytes(SnapshotStore::LATEST_KEY, doc.clone()).await.unwrap();
        let got = store.get_json_bytes(SnapshotStore::LATEST_KEY).await.unwrap();
        assert_eq!(got, doc);
    }

    #[tokio::test]
    async fn list_epochs_sorted_and_ignores_non_epoch_keys() {
        let store = SnapshotStore::with_store(Arc::new(InMemory::new()));
        store.put_bytes(&SnapshotStore::artifact_key(3, "a"), vec![1]).await.unwrap();
        store.put_bytes(&SnapshotStore::artifact_key(1, "a"), vec![1]).await.unwrap();
        store.put_bytes(&SnapshotStore::artifact_key(10, "a"), vec![1]).await.unwrap();
        // non-epoch directories and the pointer object must be ignored
        store.put_bytes("notepoch/a", vec![1]).await.unwrap();
        store.put_bytes("epoch-nan/a", vec![1]).await.unwrap();
        store.put_json_bytes(SnapshotStore::LATEST_KEY, vec![b'{']).await.unwrap();

        assert_eq!(store.list_epochs().await.unwrap(), vec![1, 3, 10]);
    }

    #[tokio::test]
    async fn list_epochs_via_file_url() {
        // exercises the non-empty key prefix produced by open()
        let remote = tempdir().unwrap();
        let store = file_store(remote.path());
        store.put_bytes(&SnapshotStore::artifact_key(2, "a"), vec![1]).await.unwrap();
        store.put_bytes(&SnapshotStore::artifact_key(9, "a"), vec![1]).await.unwrap();

        assert_eq!(store.list_epochs().await.unwrap(), vec![2, 9]);
    }

    #[tokio::test]
    async fn delete_epoch_dir_removes_all_objects() {
        let store = SnapshotStore::with_store(Arc::new(InMemory::new()));
        store.put_bytes(&SnapshotStore::artifact_key(7, "a"), vec![1]).await.unwrap();
        store.put_bytes(&SnapshotStore::artifact_key(7, "b"), vec![2]).await.unwrap();
        store.put_bytes(&SnapshotStore::manifest_key(7), vec![b'{']).await.unwrap();
        assert_eq!(store.list_epochs().await.unwrap(), vec![7]);

        store.delete_epoch_dir(7).await.unwrap();
        assert!(store.list_epochs().await.unwrap().is_empty());
        // deleting an absent directory is a no-op
        store.delete_epoch_dir(7).await.unwrap();
    }

    #[tokio::test]
    async fn prune_keeps_newest_and_never_deletes_protected() {
        let store = SnapshotStore::with_store(Arc::new(InMemory::new()));
        for epoch in 1..=5 {
            store.put_bytes(&SnapshotStore::artifact_key(epoch, "d"), vec![1]).await.unwrap();
        }

        // keep newest 2 (=4,5); of the older candidates 1,2,3 the protected epoch 1 survives
        let deleted = store.prune(2, Some(1)).await.unwrap();
        assert_eq!(deleted, vec![2, 3]);
        assert_eq!(store.list_epochs().await.unwrap(), vec![1, 4, 5]);
    }

    #[tokio::test]
    async fn prune_without_protection_deletes_all_but_newest() {
        let store = SnapshotStore::with_store(Arc::new(InMemory::new()));
        for epoch in 1..=4 {
            store.put_bytes(&SnapshotStore::artifact_key(epoch, "d"), vec![1]).await.unwrap();
        }

        let deleted = store.prune(1, None).await.unwrap();
        assert_eq!(deleted, vec![1, 2, 3]);
        assert_eq!(store.list_epochs().await.unwrap(), vec![4]);
    }

    #[test]
    fn zstd_round_trip() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("in.bin");
        let compressed = dir.path().join("in.zst");
        let out = dir.path().join("out.bin");

        let data: Vec<u8> = (0..1_000_000u32).map(|i| (i % 253) as u8).collect();
        std::fs::write(&src, &data).unwrap();

        let (uncompressed, written) = SnapshotStore::compress_file(&src, &compressed).unwrap();
        assert_eq!(uncompressed, data.len() as u64);
        assert!(written > 0);

        let decompressed =
            SnapshotStore::decompress_file_capped(&compressed, &out, 10 * 1024 * 1024).unwrap();
        assert_eq!(decompressed, data.len() as u64);
        assert_eq!(std::fs::read(&out).unwrap(), data);
    }

    #[test]
    fn zstd_cap_removes_partial_output() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("in.bin");
        let compressed = dir.path().join("in.zst");
        let out = dir.path().join("out.bin");

        // highly compressible: 4 MiB of zeros shrinks to a tiny frame
        let data = vec![0u8; 4 * 1024 * 1024];
        std::fs::write(&src, &data).unwrap();
        let (_uncompressed, written) = SnapshotStore::compress_file(&src, &compressed).unwrap();
        assert!(written < data.len() as u64);

        // a cap far below the true decompressed size must be enforced
        let err = SnapshotStore::decompress_file_capped(&compressed, &out, 1024).unwrap_err();
        assert!(matches!(err, SnapshotError::Integrity(_)));
        assert!(!out.exists());
    }
}
