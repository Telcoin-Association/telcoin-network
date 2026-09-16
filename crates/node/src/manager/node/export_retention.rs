//! Serialized publication and bounded retention of completed state exports.

use parking_lot::Mutex;
use std::{cmp::Reverse, fs, io, num::NonZeroUsize, path::PathBuf, sync::Arc};
use tn_types::Epoch;
use tracing::{info, warn};

/// The result of deciding whether a completed temporary export is still worth publishing.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum PublishOutcome {
    /// The temporary directory was atomically published and retention was applied.
    Published,
    /// Enough newer completed exports already exist; the temporary directory is untouched.
    Superseded,
}

/// The state-export root, retention policy, and publication lock shared by all export tasks.
#[derive(Clone, Debug)]
pub(super) struct StateExportRetention {
    /// Parent directory of canonical epoch directories and unfinished temporary exports.
    root: PathBuf,
    /// Number of newest completed exports to retain, or no limit.
    keep: Option<NonZeroUsize>,
    /// Serializes startup cleanup with publication and cleanup by overlapping exports.
    gate: Arc<Mutex<()>>,
    /// Gauge handle registered on the caller thread and updated by blocking workers.
    completed_bundles: metrics::Gauge,
}

impl StateExportRetention {
    /// Creates a retention policy whose clones share the same publication lock.
    pub(super) fn new(root: PathBuf, keep: Option<NonZeroUsize>) -> Self {
        Self {
            root,
            keep,
            gate: Arc::new(Mutex::new(())),
            completed_bundles: metrics::gauge!("tn_state_export_completed_bundles"),
        }
    }

    /// Applies startup retention on a blocking worker, logging failures without stopping the node.
    pub(super) async fn prune(&self) {
        let retention = self.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let _guard = retention.gate.lock();
            retention.completed_epochs().map(|epochs| retention.prune_completed(epochs))
        })
        .await
        .map_err(io::Error::other)
        .and_then(std::convert::identity)
        .inspect_err(|error| {
            warn!(target: "tn::snapshot", ?error, root = %self.root.display(), "Unable to prune completed state exports");
        });
    }

    /// Publishes an export and prunes older bundles under one blocking-worker lock.
    ///
    /// A skipped export or failed scan/rename cannot prune any completed bundle. The caller owns
    /// cleanup of the temporary directory if publication fails or is superseded.
    pub(super) async fn publish(
        &self,
        tmp_dir: PathBuf,
        epoch: Epoch,
    ) -> io::Result<PublishOutcome> {
        let retention = self.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = retention.gate.lock();
            let mut completed = retention.completed_epochs()?;
            if retention.keep.is_some_and(|keep| {
                completed.iter().filter(|completed_epoch| **completed_epoch > epoch).count()
                    >= keep.get()
            }) {
                Ok(PublishOutcome::Superseded)
            } else {
                fs::rename(tmp_dir, retention.root.join(format!("epoch-{epoch}")))?;
                // Reuse the locked scan so the publication precheck and pruning rank the same set.
                // Rename can replace an empty destination, so include its epoch exactly once.
                completed.retain(|completed_epoch| *completed_epoch != epoch);
                completed.push(epoch);
                retention.prune_completed(completed);
                Ok(PublishOutcome::Published)
            }
        })
        .await
        .map_err(io::Error::other)?
    }

    /// Lists only real direct-child directories with canonical decimal epoch names.
    ///
    /// An unreadable entry fails the entire scan, preventing decisions from an incomplete view.
    /// A missing root is an empty export history, including before the first export on startup.
    fn completed_epochs(&self) -> io::Result<Vec<Epoch>> {
        let entries = fs::read_dir(&self.root)
            .map(Some)
            .or_else(|error| {
                if error.kind() == io::ErrorKind::NotFound {
                    Ok(None)
                } else {
                    Err(error)
                }
            })
            .inspect_err(|error| {
                warn!(target: "tn::snapshot", ?error, root = %self.root.display(), "Unable to scan completed state exports");
            })?;
        entries
            .into_iter()
            .flatten()
            .map(|entry| {
                entry.and_then(|entry| {
                    entry.file_type().map(|file_type| {
                        file_type
                            .is_dir()
                            .then(|| {
                                entry.file_name().to_str().and_then(|name| {
                                    name.strip_prefix("epoch-").and_then(|digits| {
                                        digits
                                            .parse::<Epoch>()
                                            .ok()
                                            .filter(|epoch| epoch.to_string() == digits)
                                    })
                                })
                            })
                            .flatten()
                    })
                })
            })
            .collect::<io::Result<Vec<Option<Epoch>>>>()
            .map(|epochs| epochs.into_iter().flatten().collect())
            .inspect_err(|error| {
                warn!(target: "tn::snapshot", ?error, root = %self.root.display(), "Unable to read a state-export entry");
            })
    }

    /// Removes only older completed directories and records the count, including failed removals.
    fn prune_completed(&self, mut completed: Vec<Epoch>) {
        completed.sort_unstable_by_key(|epoch| Reverse(*epoch));
        let remaining = completed
            .into_iter()
            .enumerate()
            .filter(|(index, epoch)| {
                self.keep.is_none_or(|keep| *index < keep.get())
                    || fs::remove_dir_all(self.root.join(format!("epoch-{epoch}")))
                        .inspect(|_| {
                            info!(target: "tn::snapshot", epoch, "Pruned completed state export");
                        })
                        .inspect_err(|error| {
                            warn!(target: "tn::snapshot", ?error, epoch, "Unable to remove an old completed state export");
                        })
                        .is_err()
            })
            .fold(0.0, |count, _| count + 1.0);
        self.completed_bundles.set(remaining);
    }
}

/// Filesystem regressions for retention boundaries, publication failures, and overlapping exports.
#[cfg(test)]
mod tests {
    use super::*;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use std::path::Path;

    /// Creates populated canonical bundles so rename cannot silently replace an existing bundle.
    fn create_bundles(root: &Path, epochs: &[Epoch]) -> io::Result<()> {
        epochs.iter().try_for_each(|epoch| {
            let directory = root.join(format!("epoch-{epoch}"));
            fs::create_dir(&directory)?;
            fs::write(directory.join("manifest"), epoch.to_string())
        })
    }

    /// Reads completed epochs in numeric order for filesystem assertions.
    fn retained_epochs(retention: &StateExportRetention) -> io::Result<Vec<Epoch>> {
        retention.completed_epochs().map(|mut epochs| {
            epochs.sort_unstable();
            epochs
        })
    }

    /// Reads the completed-bundle gauge from a recorder bound before blocking tasks start.
    fn recorded_count(recorder: &DebuggingRecorder) -> Option<f64> {
        recorder.snapshotter().snapshot().into_vec().into_iter().find_map(|(key, _, _, value)| {
            (key.key().name() == "tn_state_export_completed_bundles")
                .then_some(match value {
                    DebugValue::Gauge(count) => Some(count.0),
                    DebugValue::Counter(_) | DebugValue::Histogram(_) => None,
                })
                .flatten()
        })
    }

    /// Startup honors numeric order, strict names, zero/MAX epochs, unlimited retention, and
    /// gauges.
    #[tokio::test]
    async fn retention_prunes_only_canonical_newest_bundles() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let recorder = DebuggingRecorder::new();
        create_bundles(root.path(), &[0, 2, 9, 10, Epoch::MAX])?;
        let unrelated = [
            "epoch-01",
            "epoch-+1",
            "epoch--1",
            "epoch-4294967296",
            "epoch-",
            "epoch-1.tmp",
            "epoch-123.tmp",
            "0",
            "misc",
        ];
        unrelated.iter().try_for_each(|name| fs::create_dir(root.path().join(name)))?;
        fs::write(root.path().join("epoch-100"), b"unrelated file")?;
        #[cfg(unix)]
        let symlink_target = tempfile::tempdir()?;
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(symlink_target.path(), root.path().join("epoch-1"))?;
            std::os::unix::fs::symlink(symlink_target.path(), root.path().join("epoch-101"))?;
        }

        let unlimited = metrics::with_local_recorder(&recorder, || {
            StateExportRetention::new(root.path().to_path_buf(), None)
        });
        unlimited.prune().await;
        assert_eq!(retained_epochs(&unlimited)?, vec![0, 2, 9, 10, Epoch::MAX]);
        assert_eq!(recorded_count(&recorder), Some(5.0));
        let generous = metrics::with_local_recorder(&recorder, || {
            StateExportRetention::new(root.path().to_path_buf(), NonZeroUsize::new(8))
        });
        generous.prune().await;
        assert_eq!(retained_epochs(&generous)?, vec![0, 2, 9, 10, Epoch::MAX]);

        let bounded = metrics::with_local_recorder(&recorder, || {
            StateExportRetention::new(root.path().to_path_buf(), NonZeroUsize::new(2))
        });
        bounded.prune().await;
        assert_eq!(retained_epochs(&bounded)?, vec![10, Epoch::MAX]);
        assert_eq!(recorded_count(&recorder), Some(2.0));
        assert!(unrelated.iter().all(|name| root.path().join(name).is_dir()));
        assert_eq!(fs::read(root.path().join("epoch-100"))?, b"unrelated file");
        #[cfg(unix)]
        {
            assert!(fs::symlink_metadata(root.path().join("epoch-1"))?.file_type().is_symlink());
            assert!(fs::symlink_metadata(root.path().join("epoch-101"))?.file_type().is_symlink());
            assert!(symlink_target.path().is_dir());
        }
        let newest = metrics::with_local_recorder(&recorder, || {
            StateExportRetention::new(root.path().to_path_buf(), NonZeroUsize::new(1))
        });
        newest.prune().await;
        assert_eq!(retained_epochs(&newest)?, vec![Epoch::MAX]);
        assert_eq!(recorded_count(&recorder), Some(1.0));
        Ok(())
    }

    /// Failed publication preserves every prior bundle; successful publication prunes afterward.
    #[tokio::test]
    async fn retention_publish_prunes_only_after_successful_rename() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let recorder = DebuggingRecorder::new();
        create_bundles(root.path(), &[1, 2, 3])?;
        let retention = metrics::with_local_recorder(&recorder, || {
            StateExportRetention::new(root.path().to_path_buf(), NonZeroUsize::new(2))
        });
        let temporary = root.path().join("epoch-4.tmp");
        fs::create_dir(&temporary)?;
        fs::write(temporary.join("manifest"), b"candidate")?;
        fs::write(root.path().join("epoch-4"), b"destination collision")?;
        assert!(retention.publish(temporary.clone(), 4).await.is_err());
        assert_eq!(retained_epochs(&retention)?, vec![1, 2, 3]);
        assert_eq!(fs::read(temporary.join("manifest"))?, b"candidate");
        assert_eq!(fs::read(root.path().join("epoch-4"))?, b"destination collision");

        fs::remove_file(root.path().join("epoch-4"))?;
        assert_eq!(retention.publish(temporary.clone(), 4).await?, PublishOutcome::Published);
        assert!(!temporary.exists());
        assert_eq!(retained_epochs(&retention)?, vec![3, 4]);
        assert_eq!(fs::read(root.path().join("epoch-4/manifest"))?, b"candidate");
        assert_eq!(recorded_count(&recorder), Some(2.0));

        // Replacing an existing empty epoch directory must not count or delete that epoch twice.
        fs::remove_file(root.path().join("epoch-4/manifest"))?;
        fs::create_dir(&temporary)?;
        fs::write(temporary.join("manifest"), b"replacement")?;
        assert_eq!(retention.publish(temporary, 4).await?, PublishOutcome::Published);
        assert_eq!(retained_epochs(&retention)?, vec![3, 4]);
        assert_eq!(fs::read(root.path().join("epoch-4/manifest"))?, b"replacement");
        assert_eq!(recorded_count(&recorder), Some(2.0));
        Ok(())
    }

    /// A stale completion cannot evict newer bundles or trigger pruning of an existing backlog.
    #[tokio::test]
    async fn retention_superseded_export_preserves_completed_and_temporary_bundles(
    ) -> io::Result<()> {
        let root = tempfile::tempdir()?;
        create_bundles(root.path(), &[0, 8, 9])?;
        let retention = StateExportRetention::new(root.path().to_path_buf(), NonZeroUsize::new(2));
        let temporary = root.path().join("epoch-7.tmp");
        fs::create_dir(&temporary)?;
        fs::write(temporary.join("manifest"), b"stale candidate")?;
        assert_eq!(retention.publish(temporary.clone(), 7).await?, PublishOutcome::Superseded);
        assert_eq!(retained_epochs(&retention)?, vec![0, 8, 9]);
        assert_eq!(fs::read(temporary.join("manifest"))?, b"stale candidate");
        assert!(!root.path().join("epoch-7").exists());
        Ok(())
    }

    /// Concurrent publications and a delayed old export converge on the newest numeric epochs.
    #[tokio::test]
    async fn retention_serializes_concurrent_and_out_of_order_exports() -> io::Result<()> {
        let root = tempfile::tempdir()?;
        let retention = StateExportRetention::new(root.path().to_path_buf(), NonZeroUsize::new(2));
        [1, 2, 3, 4]
            .iter()
            .try_for_each(|epoch| fs::create_dir(root.path().join(format!("epoch-{epoch}.tmp"))))?;
        let other = retention.clone();
        let (second, third, fourth, ()) = tokio::join!(
            retention.publish(root.path().join("epoch-2.tmp"), 2),
            other.publish(root.path().join("epoch-3.tmp"), 3),
            retention.publish(root.path().join("epoch-4.tmp"), 4),
            other.prune(),
        );
        second?;
        assert_eq!(third?, PublishOutcome::Published);
        assert_eq!(fourth?, PublishOutcome::Published);
        assert_eq!(retained_epochs(&retention)?, vec![3, 4]);
        assert_eq!(
            retention.publish(root.path().join("epoch-1.tmp"), 1).await?,
            PublishOutcome::Superseded
        );
        assert!(root.path().join("epoch-1.tmp").is_dir());
        assert_eq!(retained_epochs(&retention)?, vec![3, 4]);
        Ok(())
    }

    /// Missing roots are empty histories, and invalid roots cannot publish or delete the candidate.
    #[tokio::test]
    async fn retention_handles_missing_and_invalid_roots() -> io::Result<()> {
        let parent = tempfile::tempdir()?;
        let recorder = DebuggingRecorder::new();
        let root = parent.path().join("state_exports");
        let retention = metrics::with_local_recorder(&recorder, || {
            StateExportRetention::new(root.clone(), NonZeroUsize::new(1))
        });
        retention.completed_bundles.set(123.0);
        retention.prune().await;
        assert!(!root.exists());
        assert_eq!(recorded_count(&recorder), Some(0.0));
        fs::write(&root, b"not a directory")?;
        let temporary = parent.path().join("epoch-1.tmp");
        fs::create_dir(&temporary)?;
        retention.prune().await;
        assert!(retention.publish(temporary.clone(), 1).await.is_err());
        assert!(temporary.is_dir());
        assert_eq!(fs::read(root)?, b"not a directory");
        Ok(())
    }
}
