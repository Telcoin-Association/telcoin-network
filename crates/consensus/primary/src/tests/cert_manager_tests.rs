//! Tests for the cert manager.

use super::CertificateManager;
use crate::{error::CertManagerError, ConsensusBus};
use assert_matches::assert_matches;
use std::collections::BTreeSet;
use tn_primary::test_utils::make_optimal_signed_certificates;
use tn_storage::{mem_db::MemDatabase, CertificateStore};
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{BlsSignature, Certificate, Hash as _, SignatureVerificationState, TnReceiver};

struct TestTypes<DB = MemDatabase> {
    /// The CertificateManager
    manager: CertificateManager<DB>,
    /// The committee fixture.
    fixture: CommitteeFixture<DB>,
}

fn create_test_types() -> TestTypes<MemDatabase> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let cb = ConsensusBus::new();
    let primary = fixture.authorities().last().unwrap();

    // for validator
    let config = primary.consensus_config();

    let manager = CertificateManager::new(config, cb);

    TestTypes { manager, fixture }
}

#[tokio::test]
async fn test_unverified_certificate_fails() -> eyre::Result<()> {
    let TestTypes { mut manager, fixture, .. } = create_test_types();

    let unverified = fixture.unverified_cert_from_last_authority();
    assert!(manager.process_verified_certificates(vec![unverified]).await.is_err());

    Ok(())
}

#[tokio::test]
async fn test_accept_pending_certs() -> eyre::Result<()> {
    let TestTypes { mut manager, fixture, .. } = create_test_types();
    let committee = fixture.committee();
    let num_authorities = fixture.num_authorities();

    // make certs
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (certificates, _) =
        make_optimal_signed_certificates(1..=5, &genesis, &committee, keys.as_slice());

    // all certs
    let certs: Vec<_> = certificates
        .into_iter()
        .map(|mut c| {
            c.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
                c.aggregated_signature().expect("signature valid"),
            ));
            c
        })
        .collect();

    // separate first round (4 certs) and later rounds
    let mut first_round = certs; // for readability
    let later_rounds = first_round.split_off(num_authorities);
    let expected_pending_len = later_rounds.len();

    // try to process certs - all should be pending
    let expected_last_digest = later_rounds.last().expect("at least one cert").digest();
    let res = manager.process_verified_certificates(later_rounds).await;

    // expect all certs to process and error to reference last digest processed
    assert_matches!(res, Err(CertManagerError::Pending(digest)) if digest == expected_last_digest);

    // later_rounds should be pending
    assert_eq!(expected_pending_len, manager.pending.num_pending());
    Ok(())
}

/// A certificate manager on a fresh committee, a receiver of what it forwards to consensus, and
/// signed certificates for rounds 1 and 2.
struct ReplaceCopies<R> {
    manager: CertificateManager<MemDatabase>,
    store: MemDatabase,
    new_certificates: R,
    round1: Vec<Certificate>,
    round2: Vec<Certificate>,
}

fn replace_copies() -> ReplaceCopies<impl TnReceiver<Certificate>> {
    let fixture = CommitteeFixture::builder(MemDatabase::default).randomize_ports(true).build();
    let cb = ConsensusBus::new();
    let new_certificates = cb.subscribe_new_certificates();
    let config = fixture.authorities().last().unwrap().consensus_config();
    let store = config.node_storage().clone();
    let manager = CertificateManager::new(config, cb);
    let committee = fixture.committee();
    let genesis =
        Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
    let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
    let (mut round1, _) =
        make_optimal_signed_certificates(1..=2, &genesis, &committee, keys.as_slice());
    let round2 = round1.split_off(fixture.num_authorities());
    ReplaceCopies {
        manager,
        store,
        new_certificates,
        round1: round1.into_iter().collect(),
        round2: round2.into_iter().collect(),
    }
}

impl<R: TnReceiver<Certificate>> ReplaceCopies<R> {
    /// The stored copy of `cert`.
    fn stored(&self, cert: &Certificate) -> Certificate {
        self.store.read(cert.digest()).expect("store readable").expect("certificate stored")
    }

    /// The certificates forwarded to consensus since the last call.
    fn forwarded(&mut self) -> Vec<Certificate> {
        std::iter::from_fn(|| self.new_certificates.try_recv().ok()).collect()
    }
}

/// `cert` verified directly, with its valid signature.
fn direct(cert: &Certificate) -> Certificate {
    let mut cert = cert.clone();
    let signature = cert.aggregated_signature().expect("signed");
    cert.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(signature));
    cert
}

/// `cert` as a fetch stores it under another certificate, with a corrupted signature.
fn indirect_corrupted(cert: &Certificate) -> Certificate {
    let mut cert = cert.clone();
    cert.set_signature_verification_state(SignatureVerificationState::VerifiedIndirectly(
        BlsSignature::default(),
    ));
    cert
}

/// A fetched copy of a certificate the node already stores verified directly does not overwrite
/// it, and the certificate is not accepted a second time.
#[tokio::test]
async fn test_fetched_indirect_copy_keeps_stored_direct_copy() -> eyre::Result<()> {
    let mut cx = replace_copies();
    let cert = cx.round1[0].clone();
    let valid = cert.aggregated_signature().expect("signed");

    cx.manager.process_verified_certificates(vec![direct(&cert)]).await?;
    assert_eq!(cx.forwarded(), vec![cert.clone()], "the first copy is accepted");

    cx.manager.process_verified_certificates(vec![indirect_corrupted(&cert)]).await?;
    assert_matches!(
        cx.stored(&cert).signature_verification_state(),
        SignatureVerificationState::VerifiedDirectly(sig) if *sig == valid,
        "the directly verified stored copy stays"
    );
    assert_eq!(cx.forwarded(), Vec::new(), "a stored certificate is not accepted again");
    Ok(())
}

/// A directly verified copy of a certificate the node stores from a fetch, unchecked, replaces the
/// stored copy, without a second acceptance.
#[tokio::test]
async fn test_direct_copy_replaces_stored_indirect_copy() -> eyre::Result<()> {
    let mut cx = replace_copies();
    let cert = cx.round1[0].clone();
    let valid = cert.aggregated_signature().expect("signed");

    cx.manager.process_verified_certificates(vec![indirect_corrupted(&cert)]).await?;
    assert_eq!(cx.forwarded(), vec![cert.clone()], "the first copy is accepted");

    cx.manager.process_verified_certificates(vec![direct(&cert)]).await?;
    assert_matches!(
        cx.stored(&cert).signature_verification_state(),
        SignatureVerificationState::VerifiedDirectly(sig) if *sig == valid,
        "the directly verified copy replaces the stored one"
    );
    assert_eq!(cx.forwarded(), Vec::new(), "a stored certificate is not accepted again");
    Ok(())
}

/// A directly verified copy of a pending certificate replaces the pending copy fetched unchecked,
/// so the copy stored once the parents arrive is the checked one.
#[tokio::test]
async fn test_direct_copy_replaces_pending_indirect_copy() -> eyre::Result<()> {
    let mut cx = replace_copies();
    let cert = cx.round2[0].clone();
    let valid = cert.aggregated_signature().expect("signed");

    for copy in [indirect_corrupted(&cert), direct(&cert)] {
        let res = cx.manager.process_verified_certificates(vec![copy]).await;
        assert_matches!(res, Err(CertManagerError::Pending(digest)) if digest == cert.digest());
    }
    let parents = cx.round1.iter().map(direct).collect();
    cx.manager.process_verified_certificates(parents).await?;

    assert_matches!(
        cx.stored(&cert).signature_verification_state(),
        SignatureVerificationState::VerifiedDirectly(sig) if *sig == valid,
        "the pending copy was replaced by the directly verified one"
    );
    assert_eq!(cx.manager.pending.num_pending(), 0, "nothing stays pending");
    Ok(())
}

/// A fetched copy of a parent stored outside the manager, as a catching-up node stores gossip,
/// still unlocks the certificates pending on it, although the parent is not accepted again.
#[tokio::test]
async fn test_fetched_copy_of_stored_parent_unlocks_pending_children() -> eyre::Result<()> {
    let mut cx = replace_copies();
    let child = cx.round2[0].clone();

    let res = cx.manager.process_verified_certificates(vec![direct(&child)]).await;
    assert_matches!(res, Err(CertManagerError::Pending(digest)) if digest == child.digest());
    for parent in &cx.round1 {
        cx.store.write(direct(parent))?;
    }

    cx.manager.process_verified_certificates(cx.round1.iter().map(direct).collect()).await?;
    assert_eq!(cx.forwarded(), vec![child.clone()], "only the unlocked child is accepted");
    assert_eq!(cx.manager.pending.num_pending(), 0, "nothing stays pending");
    Ok(())
}
