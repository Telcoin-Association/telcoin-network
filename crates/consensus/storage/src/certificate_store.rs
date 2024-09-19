// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use fastcrypto::hash::Hash;
use std::{
    cmp::{max, Ordering},
    collections::BTreeMap,
    sync::Arc,
};
use telcoin_macros::fail_point;

use crate::{StoreResult, ROUNDS_TO_KEEP};
use narwhal_typed_store::{
    tables::{CertificateDigestByOrigin, CertificateDigestByRound, Certificates},
    traits::{Database, DbTx, DbTxMut},
};
use telcoin_sync::sync::notify_read::NotifyRead;
use tn_types::{AuthorityIdentifier, Certificate, CertificateDigest, Round};

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
        fail_point!("narwhal-store-before-write");
        let mut txn = self.db.write_txn()?;

        let id = certificate.digest();
        let round = certificate.round();
        self.save_cert(&mut txn, id, certificate)?;

        txn.commit()?;
        fail_point!("narwhal-store-after-write");
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
        fail_point!("narwhal-store-before-write");

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
        fail_point!("narwhal-store-after-write");
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
        /*
        // TODO- clean up and reduce allocations.
        // Batch checks into the cache and the certificate store.
        let digests = digests.enumerate().collect::<Vec<_>>();
        let mut found = self.cache.multi_contains(digests.iter().map(|(_, d)| *d));
        let store_lookups = digests
            .iter()
            .zip(found.iter())
            .filter_map(|((i, d), hit)| if *hit { None } else { Some((*i, *d)) })
            .collect::<Vec<_>>();
        for ((i, _d), hit) in store_lookups
            .into_iter()
            .map(|(i, d)| ((i, d), self.db.contains_key::<Certificates>(d).unwrap_or_default()))
        {
            debug_assert!(!found[i]);
            if hit {
                found[i] = true;
            }
        }
        Ok(found)
        */
    }

    /// Retrieves multiple certificates by their provided ids. The results
    /// are returned in the same sequence as the provided keys.
    pub fn read_all(
        &self,
        ids: impl IntoIterator<Item = CertificateDigest>,
    ) -> StoreResult<Vec<Option<Certificate>>> {
        ids.into_iter().map(|digest| self.db.get::<Certificates>(&digest)).collect()
        /*
        // TODO- clean up and reduce allocations.
        let mut found = HashMap::new();
        let mut missing = Vec::new();

        // first find whatever we can from our local cache
        let ids: Vec<CertificateDigest> = ids.into_iter().collect();
        for (id, certificate) in self.cache.read_all(ids.clone()) {
            if let Some(certificate) = certificate {
                found.insert(id, certificate.clone());
            } else {
                missing.push(id);
            }
        }

        // then fallback for all the misses on the storage
        let from_store = self.db.multi_get::<Certificates>(&missing)?;
        from_store.iter().zip(missing).for_each(|(certificate, id)| {
            if let Some(certificate) = certificate {
                found.insert(id, certificate.clone());
            }
        });

        Ok(ids.into_iter().map(|id| found.get(&id).cloned()).collect())
        */
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
        fail_point!("narwhal-store-before-write");
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
        fail_point!("narwhal-store-after-write");
        Ok(())
    }

    /// Deletes multiple certificates in an atomic way.
    pub fn delete_all(&self, ids: impl IntoIterator<Item = CertificateDigest>) -> StoreResult<()> {
        fail_point!("narwhal-store-before-write");
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
        fail_point!("narwhal-store-after-write");
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
        fail_point!("narwhal-store-before-write");
        let mut txn = self.db.write_txn()?;

        txn.clear_table::<Certificates>()?;
        txn.clear_table::<CertificateDigestByRound>()?;
        txn.clear_table::<CertificateDigestByOrigin>()?;

        txn.commit()?;
        fail_point!("narwhal-store-after-write");
        Ok(())
    }

    /// Checks whether the storage is empty. The main storage is
    /// being used to determine this.
    pub fn is_empty(&self) -> bool {
        self.db.is_empty::<Certificates>()
    }
}

#[cfg(test)]
mod test {
    use crate::certificate_store::CertificateStore;
    use fastcrypto::hash::Hash;
    use futures::future::join_all;
    use narwhal_typed_store::{open_db, traits::Database};
    use std::{
        collections::{BTreeSet, HashSet},
        time::Instant,
    };
    use tn_types::{
        test_utils::{temp_dir, CommitteeFixture},
        AuthorityIdentifier, Certificate, CertificateDigest,
    };

    fn new_store<DB: Database>(db: DB) -> CertificateStore<DB> {
        CertificateStore::new(db)
    }

    // helper method that creates certificates for the provided
    // number of rounds.
    fn certificates(rounds: u64) -> Vec<Certificate> {
        let fixture = CommitteeFixture::builder().build();
        let committee = fixture.committee();
        let mut current_round: Vec<_> = Certificate::genesis(&committee)
            .into_iter()
            .map(|cert| cert.header().clone())
            .collect();

        let mut result: Vec<Certificate> = Vec::new();
        for i in 0..rounds {
            let parents: BTreeSet<_> =
                current_round.iter().map(|header| fixture.certificate(header).digest()).collect();
            (_, current_round) = fixture.headers_round(i, &parents);

            result.extend(
                current_round.iter().map(|h| fixture.certificate(h)).collect::<Vec<Certificate>>(),
            );
        }

        result
    }

    #[tokio::test]
    async fn test_write_and_read() {
        let db = open_db(temp_dir());
        test_write_and_read_by_store_type(new_store(db)).await;
    }

    async fn test_write_and_read_by_store_type<DB: Database>(store: CertificateStore<DB>) {
        // GIVEN
        // create certificates for 10 rounds
        let certs = certificates(10);
        let digests = certs.iter().map(|c| c.digest()).collect::<Vec<_>>();

        // verify certs not in the store
        for cert in &certs {
            assert!(!store.contains(&cert.digest()).unwrap());
            assert!(&store.read(cert.digest()).unwrap().is_none());
        }

        let found = store.multi_contains(digests.iter()).unwrap();
        assert_eq!(found.len(), certs.len());
        for hit in found {
            assert!(!hit);
        }

        // store the certs
        for cert in &certs {
            store.write(cert.clone()).unwrap();
        }

        // verify certs in the store
        for cert in &certs {
            assert!(store.contains(&cert.digest()).unwrap());
            assert_eq!(cert, &store.read(cert.digest()).unwrap().unwrap())
        }

        let found = store.multi_contains(digests.iter()).unwrap();
        assert_eq!(found.len(), certs.len());
        for hit in found {
            assert!(hit);
        }
    }

    #[tokio::test]
    async fn test_write_all_and_read_all() {
        let db = open_db(temp_dir());
        test_write_all_and_read_all_by_store_type(new_store(db)).await;
    }

    async fn test_write_all_and_read_all_by_store_type<DB: Database>(store: CertificateStore<DB>) {
        // GIVEN
        // create certificates for 10 rounds
        let certs = certificates(10);
        let ids = certs.iter().map(|c| c.digest()).collect::<Vec<CertificateDigest>>();

        // store them in both main and secondary index
        store.write_all(certs.clone()).unwrap();

        // WHEN
        let result = store.read_all(ids).unwrap();

        // THEN
        assert_eq!(certs.len(), result.len());

        for (i, cert) in result.into_iter().enumerate() {
            let c = cert.expect("Certificate should have been found");

            assert_eq!(&c, certs.get(i).unwrap());
        }
    }

    #[tokio::test]
    async fn test_next_round_number() {
        // GIVEN
        let db = open_db(temp_dir());
        let store = new_store(db);

        // Create certificates for round 1, 2, 4, 6, 9, 10.
        let cert = certificates(1).first().unwrap().clone();
        let origin = cert.origin();
        let rounds = vec![1, 2, 4, 6, 9, 10];
        let mut certs = Vec::new();
        for r in &rounds {
            let mut c = cert.clone();
            c.header_mut().update_round(*r);
            certs.push(c);
        }

        store.write_all(certs).unwrap();

        // THEN
        let mut i = 0;
        let mut current_round = 0;
        while let Some(r) = store.next_round_number(origin, current_round).unwrap() {
            assert_eq!(rounds[i], r);
            i += 1;
            current_round = r;
        }
    }

    #[tokio::test]
    async fn test_last_two_rounds() {
        // GIVEN
        let db = open_db(temp_dir());
        let store = new_store(db);

        // create certificates for 50 rounds
        let certs = certificates(50);
        let origin = certs[0].origin();

        // store them in both main and secondary index
        store.write_all(certs).unwrap();

        // WHEN
        let result = store.last_two_rounds_certs().unwrap();
        let last_round_cert = store.last_round(origin).unwrap().unwrap();
        let last_round_number = store.last_round_number(origin).unwrap().unwrap();
        let last_round_number_not_exist =
            store.last_round_number(AuthorityIdentifier(10u16)).unwrap();
        let highest_round_number = store.highest_round_number();

        // THEN
        assert_eq!(result.len(), 8);
        assert_eq!(last_round_cert.round(), 50);
        assert_eq!(last_round_number, 50);
        assert_eq!(highest_round_number, 50);
        for certificate in result {
            assert!(
                (certificate.round() == last_round_number)
                    || (certificate.round() == last_round_number - 1)
            );
        }
        assert!(last_round_number_not_exist.is_none());
    }

    #[tokio::test]
    async fn test_last_round_in_empty_store() {
        // GIVEN
        let db = open_db(temp_dir());
        let store = new_store(db);

        // WHEN
        let result = store.last_two_rounds_certs().unwrap();
        let last_round_cert = store.last_round(AuthorityIdentifier::default()).unwrap();
        let last_round_number = store.last_round_number(AuthorityIdentifier::default()).unwrap();
        let highest_round_number = store.highest_round_number();

        // THEN
        assert!(result.is_empty());
        assert!(last_round_cert.is_none());
        assert!(last_round_number.is_none());
        assert_eq!(highest_round_number, 0);
    }

    #[tokio::test]
    async fn test_after_round() {
        // GIVEN
        let db = open_db(temp_dir());
        let store = new_store(db);
        let total_rounds = 100;

        // create certificates for 50 rounds
        let now = Instant::now();

        println!("Generating certificates");

        let certs = certificates(total_rounds);
        println!("Created certificates: {} seconds", now.elapsed().as_secs_f32());

        let now = Instant::now();
        println!("Storing certificates");

        // store them in both main and secondary index
        store.write_all(certs.clone()).unwrap();

        println!("Stored certificates: {} seconds", now.elapsed().as_secs_f32());

        // Large enough to avoid certificate store GC.
        let round_cutoff = 41;

        // now filter the certificates over round 21
        let mut certs_ids_over_cutoff_round = certs
            .into_iter()
            .filter_map(|c| if c.round() >= round_cutoff { Some(c.digest()) } else { None })
            .collect::<HashSet<_>>();

        // WHEN
        println!("Access after round {round_cutoff}, before {total_rounds}");
        let now = Instant::now();
        let result =
            store.after_round(round_cutoff).expect("Error returned while reading after_round");

        println!("Total time: {} seconds", now.elapsed().as_secs_f32());

        // THEN
        let certs_per_round = 4;
        assert_eq!(result.len() as u64, (total_rounds - round_cutoff + 1) * certs_per_round);

        // AND result certificates should be returned in increasing order
        let mut last_round = 0;
        for certificate in result {
            assert!(certificate.round() >= last_round);
            last_round = certificate.round();

            // should be amongst the certificates of the cut-off round
            assert!(certs_ids_over_cutoff_round.remove(&certificate.digest()));
        }

        // AND none should be left in the original set
        assert!(certs_ids_over_cutoff_round.is_empty());

        // WHEN get rounds per origin.
        let rounds = store
            .origins_after_round(round_cutoff)
            .expect("Error returned while reading origins_after_round");
        assert_eq!(rounds.len(), (total_rounds - round_cutoff + 1) as usize);
        for origins in rounds.values() {
            assert_eq!(origins.len(), 4);
        }
    }

    #[tokio::test]
    async fn test_notify_read() {
        let db = open_db(temp_dir());
        let store = new_store(db);

        // run the tests a few times
        for _ in 0..10 {
            let mut certs = certificates(3);
            let mut ids = certs.iter().map(|c| c.digest()).collect::<Vec<CertificateDigest>>();

            let cloned_store = store.clone();

            // now populate a certificate
            let c1 = certs.remove(0);
            store.write(c1.clone()).unwrap();

            // spawn a task to notify_read on the certificate's id - we testing
            // the scenario where the value is already populated before
            // calling the notify read.
            let id = ids.remove(0);
            let handle_1 = tokio::spawn(async move { cloned_store.notify_read(id).await });

            // now spawn a series of tasks before writing anything in store
            let mut handles = vec![];
            for id in ids {
                let cloned_store = store.clone();
                let handle = tokio::spawn(async move {
                    // wait until the certificate gets populated
                    cloned_store.notify_read(id).await
                });

                handles.push(handle)
            }

            // and populate the rest with a write_all
            store.write_all(certs).unwrap();

            // now wait on handle an assert result for a single certificate
            let received_certificate =
                handle_1.await.expect("error").expect("shouldn't receive store error");

            assert_eq!(received_certificate, c1);

            let result = join_all(handles).await;
            for r in result {
                let certificate_result = r.unwrap();
                assert!(certificate_result.is_ok());
            }

            // clear the store before next run
            store.clear().unwrap();
        }
    }

    #[tokio::test]
    async fn test_write_all_and_clear() {
        let db = open_db(temp_dir());
        let store = new_store(db);

        // create certificates for 10 rounds
        let certs = certificates(10);

        // store them in both main and secondary index
        store.write_all(certs).unwrap();

        // confirm store is not empty
        assert!(!store.is_empty());

        // now clear the store
        store.clear().unwrap();

        // now confirm that store is empty
        assert!(store.is_empty());
    }

    /// Test new store.
    ///
    /// workaround for error:
    /// ```text
    /// thread 'certificate_store::test::test_delete_by_store_type' panicked at crates/consensus/typed-store/src/metrics.rs:268:14:
    /// called `Result::unwrap()` on an `Err` value: AlreadyReg
    /// ```
    #[tokio::test]
    async fn test_delete_store() {
        let db = open_db(temp_dir());
        let store = new_store(db);
        // GIVEN
        // create certificates for 10 rounds
        let certs = certificates(10);

        // store them in both main and secondary index
        store.write_all(certs.clone()).unwrap();

        // WHEN now delete a couple of certificates
        let to_delete = certs.iter().take(2).map(|c| c.digest()).collect::<Vec<_>>();

        store.delete(to_delete[0]).unwrap();
        store.delete(to_delete[1]).unwrap();

        // THEN
        assert!(store.read(to_delete[0]).unwrap().is_none());
        assert!(store.read(to_delete[1]).unwrap().is_none());
    }

    #[tokio::test]
    async fn test_delete_all_store() {
        let db = open_db(temp_dir());
        let store = new_store(db);
        // GIVEN
        // create certificates for 10 rounds
        let certs = certificates(10);

        // store them in both main and secondary index
        store.write_all(certs.clone()).unwrap();

        // WHEN now delete a couple of certificates
        let to_delete = certs.iter().take(2).map(|c| c.digest()).collect::<Vec<_>>();

        store.delete_all(to_delete.clone()).unwrap();

        // THEN
        assert!(store.read(to_delete[0]).unwrap().is_none());
        assert!(store.read(to_delete[1]).unwrap().is_none());
    }
}
