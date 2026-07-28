//! State sync tests for primaries.

use crate::{network::MissingCertificatesRequest, state_sync::CertificateCollector};
use std::{collections::BTreeSet, num::NonZeroUsize};
use tn_storage::{mem_db::MemDatabase, CertificateStore};
use tn_test_utils_committee::CommitteeFixture;
use tn_types::{AuthorityIdentifier, Certificate, Hash as _, SignatureVerificationState};

#[test]
fn test_certificate_iterator() {
    // authority fixtures
    let fixture = CommitteeFixture::builder(MemDatabase::default)
        .randomize_ports(true)
        .committee_size(NonZeroUsize::new(4).unwrap())
        .build();
    let primary = fixture.authorities().next().expect("primary in committee fixture");
    let consensus_config = primary.consensus_config();
    let certificate_store = primary.consensus_config().node_storage().clone();

    // setup dummy data
    let mut current_round: Vec<_> = Certificate::genesis(&fixture.committee())
        .into_iter()
        .map(|cert| cert.header().clone())
        .collect();
    let mut headers = vec![];
    let total_rounds = 4;
    for i in 0..total_rounds {
        let parents: BTreeSet<_> =
            current_round.into_iter().map(|header| fixture.certificate(&header).digest()).collect();
        (_, current_round) = fixture.headers_round(i, &parents);
        headers.extend(current_round.clone());
    }
    let total_authorities = fixture.authorities().count();
    let total_certificates = total_authorities * total_rounds as usize;
    // Create certificates test data.
    let mut certificates = vec![];
    for header in headers.into_iter() {
        certificates.push(fixture.certificate(&header));
    }
    assert_eq!(certificates.len(), total_certificates);
    assert_eq!(16, total_certificates);

    // Populate certificate store such that each authority has the following rounds:
    // Authority 0: 1
    // Authority 1: 1 2
    // Authority 2: 1 2 3
    // Authority 3: 1 2 3 4
    // This is unrealistic because in practice a certificate can only be stored with 2f+1 parents
    // already in store. But this does not matter for testing here.
    let mut authorities = Vec::<AuthorityIdentifier>::new();
    for i in 0..total_authorities {
        authorities.push(certificates[i].header().author().clone());
        for j in 0..=i {
            let mut cert = certificates[i + j * total_authorities].clone();
            assert_eq!(&cert.header().author(), &authorities.last().unwrap());
            if i == 3 && j == 3 {
                // Simulating only 1 directly verified certificate (Auth 3 Round 4) being stored.
                cert.set_signature_verification_state(
                    SignatureVerificationState::VerifiedDirectly(
                        cert.aggregated_signature().expect("Invalid Signature"),
                    ),
                );
            } else {
                // Simulating some indirectly verified certificates being stored.
                cert.set_signature_verification_state(
                    SignatureVerificationState::VerifiedIndirectly(
                        cert.aggregated_signature().expect("Invalid Signature"),
                    ),
                );
            }
            certificate_store.write(cert).expect("Writing certificate to store failed");
        }
    }

    // Each case: (exclusive lower-bound round, per-authority skip rounds, expected output
    // rounds). Item 7 streams the reply and ends it explicitly, so the collector is no
    // longer size-capped: it returns the full matching set, bounded only by storage and the
    // per-request processing-time limit.
    let test_cases = vec![
        (0, vec![vec![], vec![], vec![], vec![]], vec![1_u32, 1, 1, 1, 2, 2, 2, 3, 3, 4]),
        (0, vec![vec![1u32], vec![1], vec![], vec![]], vec![1, 1, 2, 2, 2, 3, 3, 4]),
        (0, vec![vec![], vec![], vec![1], vec![1]], vec![1, 1, 2, 2, 2, 3, 3, 4]),
        (1, vec![vec![], vec![], vec![2], vec![2]], vec![2, 3, 3, 4]),
        (0, vec![vec![1], vec![1], vec![1, 2, 3], vec![1, 2, 3]], vec![2, 4]),
        (2, vec![vec![], vec![], vec![], vec![]], vec![3, 3, 4]),
        // round 2 and 4 are fetched for the last authority, skipping round 3.
        (1, vec![vec![], vec![], vec![3], vec![3]], vec![2, 2, 2, 4]),
    ];

    for (lower_bound_round, skip_rounds_vec, expected_rounds) in test_cases {
        // serialize the per-authority skip rounds (via `set_bounds`), then build the
        // streaming collector directly from the request bounds
        let req = MissingCertificatesRequest::default()
            .set_bounds(
                lower_bound_round,
                authorities
                    .clone()
                    .into_iter()
                    .zip(skip_rounds_vec.into_iter().map(|rounds| rounds.into_iter().collect()))
                    .collect(),
            )
            .expect("bounds within range");

        let collector = CertificateCollector::new(
            req.exclusive_lower_bound,
            req.skip_rounds,
            consensus_config.clone(),
        )
        .expect("certificate collector process valid request");

        let missing: Vec<u32> =
            collector.map(|cert| cert.expect("cert recovered correctly").round()).collect();

        assert_eq!(missing, expected_rounds);
    }
}
