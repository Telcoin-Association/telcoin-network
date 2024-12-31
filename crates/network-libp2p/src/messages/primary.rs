//! Messages for the primary protocol.

use crate::types::NetworkResult;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use tn_types::{AuthorityIdentifier, Certificate, CertificateDigest, Header, Round, Vote};

/// Requests from Primary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PrimaryRequest {
    /// A new certificate broadcast from peer.
    ///
    /// NOTE: expect no response
    /// TODO: gossip this instead
    NewCertificate {
        /// The certificate from this peer.
        certificate: Certificate,
    },
    /// Primary request for vote on new header.
    Vote {
        /// This primary's header for the round.
        header: Header,
        /// Parent certificates provided by the requesting peer in case the primary's peer is
        /// missing them. The peer requires parent certs in order to vote.
        parents: Vec<Certificate>,
    },
    /// Request for missing certificates.
    MissingCertificates {
        /// Inner type with specific helper methods for requesting missing certificates.
        inner: MissingCertificatesRequest,
    },
}

/// Used by the primary to fetch certificates from other primaries.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingCertificatesRequest {
    /// The request is for certificates AFTER this round (non-inclusive). The boundary indicates
    /// the difference between the requestor's GC round and is the last round for which this peer
    /// has sufficient certificates.
    pub exclusive_lower_bound: Round,
    /// Rounds that should be skipped while processing this request (by authority). The rounds are
    /// serialized as [RoaringBitmap]s.
    pub skip_rounds: Vec<(AuthorityIdentifier, Vec<u8>)>,
    /// The maximum number of expected certificates included in the response.
    pub max_items: usize,
}

impl MissingCertificatesRequest {
    /// Deserialize the [RoaringBitmap] representing the difference between the requesting peer's
    /// lower boundary and their GC round.
    pub fn get_bounds(
        &self,
    ) -> NetworkResult<(Round, BTreeMap<AuthorityIdentifier, BTreeSet<Round>>)> {
        let skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>> = self
            .skip_rounds
            .iter()
            .map(|(k, serialized)| {
                let rounds = RoaringBitmap::deserialize_from(&serialized[..])?
                    .into_iter()
                    .map(|r| self.exclusive_lower_bound + r as Round)
                    .collect::<BTreeSet<Round>>();
                Ok((*k, rounds))
            })
            .collect::<NetworkResult<BTreeMap<_, _>>>()?;
        Ok((self.exclusive_lower_bound, skip_rounds))
    }

    /// Set the bounds for requesting missing certificates based on the current GC round.
    ///
    /// This method specifies which rounds should be skipped because they are already in storage.
    pub fn set_bounds(
        mut self,
        gc_round: Round,
        skip_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>>,
    ) -> NetworkResult<Self> {
        self.exclusive_lower_bound = gc_round;
        self.skip_rounds = skip_rounds
            .into_iter()
            .map(|(k, rounds)| {
                let mut serialized = Vec::new();
                rounds
                    .into_iter()
                    .map(|v| u32::try_from(v - gc_round))
                    .collect::<Result<RoaringBitmap, std::num::TryFromIntError>>()?
                    .serialize_into(&mut serialized)?;

                Ok((k, serialized))
            })
            .collect::<NetworkResult<Vec<_>>>()?;

        Ok(self)
    }

    /// Specify the maximum number of expected certificates in the peer's response.
    pub fn set_max_items(mut self, max_items: usize) -> Self {
        self.max_items = max_items;
        self
    }
}

//
//
//=== Response types
//
//

/// Response to primary requests.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum PrimaryResponse {
    /// The peer's vote if successfule. If the peer was unable to verify parents, the missing
    /// certificate digests are included.
    Vote {
        /// The vote, if the peer considered the proposed header valid.
        vote: Option<Vote>,
        /// Missing certificate digests for peer to vote.
        ///
        /// The peer needs to process these certificates before it can vote for this primary's
        /// header.
        missing: Vec<CertificateDigest>,
    },
    /// The requested missing certificates.
    MissingCertificates {
        /// The collection of missing certificates.
        certificates: Vec<Certificate>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_missing_certs_request() {
        let max = 10;
        let expected_gc_round = 3;
        let expected_skip_rounds: BTreeMap<_, _> = [
            (AuthorityIdentifier(0), BTreeSet::from([4, 5, 6, 7])),
            (AuthorityIdentifier(2), BTreeSet::from([6, 7, 8])),
        ]
        .into_iter()
        .collect();
        let missing_req = MissingCertificatesRequest::default()
            .set_bounds(expected_gc_round, expected_skip_rounds.clone())
            .expect("boundary set")
            .set_max_items(max);
        let (decoded_gc_round, decoded_skip_rounds) =
            missing_req.get_bounds().expect("decode missing bounds");
        assert_eq!(expected_gc_round, decoded_gc_round);
        assert_eq!(expected_skip_rounds, decoded_skip_rounds);
    }
}
