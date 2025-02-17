//! NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.

use fastcrypto::hash::Hash;
use std::{
    cmp::{max, Ordering},
    collections::BTreeMap,
    sync::Arc,
};
use tn_utils::fail_point;

use crate::{
    tables::{CertificateDigestByOrigin, CertificateDigestByRound, Certificates},
    StoreResult, ROUNDS_TO_KEEP,
};
use tn_types::{AuthorityIdentifier, Certificate, CertificateDigest, Database, DbTx, DbTxMut, Round};
use tn_utils::sync::notify_read::NotifyRead;

/// The main storage when we have to deal with certificates.
///
/// It maintains two storages, one main which saves the certificates by their ids, and a
/// secondary one which acts as an index to allow us fast retrieval based
/// for queries based in certificate rounds.
/// It also offers pub/sub capabilities in write events. By using the
/// `notify_read` someone can wait to hear until a certificate by a specific
/// id has been written in storage.
/// This uses the following tables in the DB:
/// - Certificates: The basic digest to certificate store.
/// - CertificateDigestByRound: A secondary index that keeps the certificate digest ids by the
///   certificate rounds. Certificate origin is used to produce unique keys. This helps us to
///   perform range requests based on rounds. We avoid storing again the certificate here to not
///   waste space. To dereference we use the certificates_by_id storage.
/// - CertificateDigestByOrigin: A secondary index that keeps the certificate digest ids by the
///   certificate origins. Certificate rounds are used to produce unique keys. This helps us to
///   perform range requests based on rounds. We avoid storing again the certificate here to not
///   waste space. To dereference we use the certificates_by_id storage.
#[derive(Clone)]
pub struct CertificateStore<DB> {
    /// The storage DB
    db: DB,
    /// The pub/sub to notify for a write that happened for a certificate digest id
    notify_subscribers: Arc<NotifyRead<CertificateDigest, Certificate>>,
}

impl<DB: Database> CertificateStore<DB> {
    pub fn new(db: DB) -> CertificateStore<DB> {
        Self { db, notify_subscribers: Arc::new(NotifyRead::new()) }
    }

    /// Save a cert using an open txn.
    fn save_cert<TX: DbTxMut>(
        &self,
        txn: &mut TX,
        digest: CertificateDigest,
        certificate: Certificate,
    ) -> StoreResult<()> {
        txn.insert::<Certificates>(&digest, &certificate)?;

        // write the certificates id by their rounds
        let key = (certificate.round(), certificate.origin());
        txn.insert::<CertificateDigestByRound>(&key, &digest)?;

        // write the certificates id by their origins
        let key = (certificate.origin(), certificate.round());
        txn.insert::<CertificateDigestByOrigin>(&key, &digest)?;

        self.notify_subscribers.notify(&digest, &certificate);

        Ok(())
    }

    /// Inserts a certificate to the store
    pub fn write(&self, certificate: Certificate) -> StoreResult<()> {
        fail_point!("certificate-store-before-write");
        let mut txn = self.db.write_txn()?;

        let id = certificate.digest();
        let round = certificate.round();
        self.save_cert(&mut txn, id, certificate)?;

        txn.commit()?;
        fail_point!("certificate-store-after-write");
        self.gc_rounds(round)?;
        Ok(())
    }

    /// Inserts multiple certificates in the storage. This is an atomic operation.
    /// In the end it notifies any subscribers that are waiting to hear for the
    /// value.
    pub fn write_all(
        &self,
        certificates: impl IntoIterator<Item = Certificate>,
    ) -> StoreResult<()> {
        fail_point!("certificate-store-before-write");

        let mut txn = self.db.write_txn()?;
        let mut round = 0;
        for certificate in certificates {
            let digest = certificate.digest();
            round = max(round, certificate.round());
            if let Err(e) = self.save_cert(&mut txn, digest, certificate) {
                tracing::error!("Failed to write certificate for {digest} due to error {e}.");
                return Err(e);
            }
        }

        txn.commit()?;
        self.gc_rounds(round)?;
        fail_point!("certificate-store-after-write");
        Ok(())
    }

    /// Retrieves a certificate from the store. If not found
    /// then None is returned as result.
    pub fn read(&self, id: CertificateDigest) -> StoreResult<Option<Certificate>> {
        self.db.get::<Certificates>(&id)
    }

    /// Retrieves a certificate from the store by round and authority.
    /// If not found, None is returned as result.
    pub fn read_by_index(
        &self,
        origin: AuthorityIdentifier,
        round: Round,
    ) -> StoreResult<Option<Certificate>> {
        match self.db.get::<CertificateDigestByOrigin>(&(origin, round))? {
            Some(d) => self.read(d),
            None => Ok(None),
        }
    }

    pub fn contains(&self, digest: &CertificateDigest) -> StoreResult<bool> {
        self.db.contains_key::<Certificates>(digest)
    }

    pub fn multi_contains<'a>(
        &self,
        digests: impl Iterator<Item = &'a CertificateDigest>,
    ) -> StoreResult<Vec<bool>> {
        digests.map(|digest| self.db.contains_key::<Certificates>(digest)).collect()
    }

    /// Retrieves multiple certificates by their provided ids. The results
    /// are returned in the same sequence as the provided keys.
    pub fn read_all(
        &self,
        ids: impl IntoIterator<Item = CertificateDigest>,
    ) -> StoreResult<Vec<Option<Certificate>>> {
        ids.into_iter().map(|digest| self.db.get::<Certificates>(&digest)).collect()
    }

    /// Waits to get notified until the requested certificate becomes available
    pub async fn notify_read(&self, id: CertificateDigest) -> StoreResult<Certificate> {
        // we register our interest to be notified with the value
        let receiver = self.notify_subscribers.register_one(&id);

        // let's read the value because we might have missed the opportunity
        // to get notified about it
        if let Ok(Some(cert)) = self.read(id) {
            // notify any obligations - and remove the entries
            self.notify_subscribers.notify(&id, &cert);

            // reply directly
            return Ok(cert);
        }

        // now wait to hear back the result
        let result = receiver.await;

        Ok(result)
    }

    /// Deletes a single certificate by its digest.
    pub fn delete(&self, id: CertificateDigest) -> StoreResult<()> {
        fail_point!("certificate-store-before-write");
        let mut txn = self.db.write_txn()?;
        // first read the certificate to get the round - we'll need in order
        // to delete the secondary index
        let cert = match self.read(id)? {
            Some(cert) => cert,
            None => return Ok(()),
        };

        // delete the certificate by its id
        txn.remove::<Certificates>(&id)?;

        // delete the certificate index by its round
        let key = (cert.round(), cert.origin());

        txn.remove::<CertificateDigestByRound>(&key)?;

        txn.commit()?;
        fail_point!("certificate-store-after-write");
        Ok(())
    }

    /// Deletes multiple certificates in an atomic way.
    pub fn delete_all(&self, ids: impl IntoIterator<Item = CertificateDigest>) -> StoreResult<()> {
        fail_point!("certificate-store-before-write");
        let mut txn = self.db.write_txn()?;

        for id in ids {
            let mut del_certs = false;
            // delete the certificates from the secondary index
            if let Some(cert) = self.read(id)? {
                del_certs = true;
                txn.remove::<CertificateDigestByRound>(&(cert.round(), cert.origin()))?;
            }
            if !del_certs {
                return Ok(());
            }

            // delete the certificates by its ids
            txn.remove::<Certificates>(&id)?;
        }

        txn.commit()?;
        fail_point!("certificate-store-after-write");
        Ok(())
    }

    /// Retrieves all the certificates with round >= the provided round.
    /// The result is returned with certificates sorted in round asc order
    pub fn after_round(&self, round: Round) -> StoreResult<Vec<Certificate>> {
        let txn = self.db.read_txn()?;
        // Skip to a row at or before the requested round.
        // TODO: Add a more efficient seek method to typed store.
        let iter = if round > 0 {
            self.db
                .skip_to::<CertificateDigestByRound>(&(round - 1, AuthorityIdentifier::default()))?
        } else {
            self.db.iter::<CertificateDigestByRound>()
        };

        let mut certs = Vec::new();
        for ((r, _), d) in iter {
            match r.cmp(&round) {
                Ordering::Equal | Ordering::Greater => {
                    // Fetch all those certificates from main storage, return an error if any one is
                    // missing.
                    if let Some(cert) = txn.get::<Certificates>(&d)? {
                        certs.push(cert);
                    } else {
                        return Err(eyre::Report::msg(format!(
                            "Certificate with some digests not found, CertificateStore invariant violation: {d}"
                        )));
                    }
                }
                Ordering::Less => {
                    continue;
                }
            }
        }
        Ok(certs)
    }

    /// Deletes all certs for a round before round.
    fn gc_rounds(&self, target_round: Round) -> StoreResult<()> {
        if target_round <= ROUNDS_TO_KEEP {
            return Ok(());
        }
        let target_round = target_round - ROUNDS_TO_KEEP;
        let mut certs = Vec::new();
        for ((round, origin), digest) in self.db.iter::<CertificateDigestByRound>() {
            if round < target_round {
                certs.push((round, origin, digest));
            } else {
                // We are done, all following rounds will be greater.
                break;
            }
        }
        let mut txn = self.db.write_txn()?;
        for (round, origin, digest) in certs {
            txn.remove::<Certificates>(&digest)?;
            txn.remove::<CertificateDigestByRound>(&(round, origin))?;
            txn.remove::<CertificateDigestByOrigin>(&(origin, round))?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Retrieves origins with certificates in each round >= the provided round.
    pub fn origins_after_round(
        &self,
        round: Round,
    ) -> StoreResult<BTreeMap<Round, Vec<AuthorityIdentifier>>> {
        // Skip to a row at or before the requested round.
        // TODO: Add a more efficient seek method to typed store.
        let iter = if round > 0 {
            self.db
                .skip_to::<CertificateDigestByRound>(&(round - 1, AuthorityIdentifier::default()))?
        } else {
            self.db.iter::<CertificateDigestByRound>()
        };

        let mut result = BTreeMap::<Round, Vec<AuthorityIdentifier>>::new();
        for ((r, origin), _) in iter {
            if r < round {
                continue;
            }
            result.entry(r).or_default().push(origin);
        }
        Ok(result)
    }

    /// Retrieves the certificates of the last round and the round before that
    pub fn last_two_rounds_certs(&self) -> StoreResult<Vec<Certificate>> {
        // starting from the last element - hence the last round - move backwards until
        // we find certificates of different round.
        let certificates_reverse = self.db.reverse_iter::<CertificateDigestByRound>();

        let mut round = 0;
        let mut certificates = Vec::new();

        for (key, digest) in certificates_reverse {
            let (certificate_round, _certificate_origin) = key;

            // We treat zero as special value (round unset) in order to
            // capture the last certificate's round.
            // We are now in a round less than the previous so we want to
            // stop consuming
            if round == 0 {
                round = certificate_round;
            } else if certificate_round < round - 1 {
                break;
            }

            let certificate = self.db.get::<Certificates>(&digest)?.ok_or_else(|| {
                eyre::Report::msg(format!(
                    "Certificate with id {} not found in main storage although it should",
                    digest
                ))
            })?;

            certificates.push(certificate);
        }

        Ok(certificates)
    }

    /// Retrieves the last certificate of the given origin.
    /// Returns None if there is no certificate for the origin.
    pub fn last_round(&self, origin: AuthorityIdentifier) -> StoreResult<Option<Certificate>> {
        let key = (origin, Round::MAX);
        if let Some(((name, _round), digest)) =
            self.db.record_prior_to::<CertificateDigestByOrigin>(&key)
        {
            if name == origin {
                return self.read(digest);
            }
        }
        Ok(None)
    }

    /// Retrieves the highest round number in the store.
    /// Returns 0 if there is no certificate in the store.
    pub fn highest_round_number(&self) -> Round {
        let ((round, _), _) =
            self.db.reverse_iter::<CertificateDigestByRound>().next().unwrap_or_default();
        round
    }

    /// Retrieves the last round number of the given origin.
    /// Returns None if there is no certificate for the origin.
    pub fn last_round_number(&self, origin: AuthorityIdentifier) -> StoreResult<Option<Round>> {
        let key = (origin, Round::MAX);
        if let Some(((name, round), _)) = self.db.record_prior_to::<CertificateDigestByOrigin>(&key)
        {
            if name == origin {
                return Ok(Some(round));
            }
        }
        Ok(None)
    }

    /// Retrieves the next round number bigger than the given round for the origin.
    /// Returns None if there is no more local certificate from the origin with bigger round.
    pub fn next_round_number(
        &self,
        origin: AuthorityIdentifier,
        round: Round,
    ) -> StoreResult<Option<Round>> {
        let key = (origin, round + 1);
        if let Some(((name, round), _)) = self.db.skip_to::<CertificateDigestByOrigin>(&key)?.next()
        {
            if name == origin {
                return Ok(Some(round));
            }
        }
        Ok(None)
    }

    /// Clears both the main storage of the certificates and the secondary index
    pub fn clear(&self) -> StoreResult<()> {
        fail_point!("certificate-store-before-write");
        let mut txn = self.db.write_txn()?;

        txn.clear_table::<Certificates>()?;
        txn.clear_table::<CertificateDigestByRound>()?;
        txn.clear_table::<CertificateDigestByOrigin>()?;

        txn.commit()?;
        fail_point!("certificate-store-after-write");
        Ok(())
    }

    /// Checks whether the storage is empty. The main storage is
    /// being used to determine this.
    pub fn is_empty(&self) -> bool {
        self.db.is_empty::<Certificates>()
    }
}

// NOTE: tests for this module are in test-utils storage_tests.rs to avoid circular dependancies.
