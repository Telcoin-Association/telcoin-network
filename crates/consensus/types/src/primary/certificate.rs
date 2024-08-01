use base64::{engine::general_purpose, Engine};
use enum_dispatch::enum_dispatch;
use fastcrypto::{
    hash::{Digest, Hash},
    traits::AggregateAuthenticator,
};
use mem_utils::MallocSizeOf;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::{collections::VecDeque, fmt};

use crate::{
    config::{AuthorityIdentifier, Committee, Epoch, Stake, WorkerCache},
    crypto::{
        self, to_intent_message, BlsAggregateSignature, BlsAggregateSignatureBytes, BlsPublicKey,
        BlsSignature, ValidatorAggregateSignature,
    },
    error::{DagError, DagResult},
    now,
    serde::NarwhalBitmap,
    Header, HeaderAPI, HeaderV1, Round, TimestampSec,
};

/// Versioned certificate. Certificates are the output of consensus.
#[derive(Clone, Serialize, Deserialize, MallocSizeOf)]
#[enum_dispatch(CertificateAPI)]
pub enum Certificate {
    /// V1 - based on sui's V2
    V1(CertificateV1),
}

// TODO: Revisit if we should not impl Default for Certificate
impl Default for Certificate {
    fn default() -> Self {
        Self::V1(CertificateV1::default())
    }
}

impl Certificate {
    /// TODO: Add version number and match on that
    pub fn genesis(committee: &Committee) -> Vec<Self> {
        CertificateV1::genesis(committee).into_iter().map(Self::V1).collect()
    }

    // /// Create genesis with header payload for [CertificateV1]
    // pub fn genesis_with_payload(
    //     committee: &Committee,
    //     batch: Batch,
    // ) -> Vec<Self> {
    //     CertificateV1::genesis_with_payload(committee, batch).into_iter().map(Self::V1).collect()
    // }

    /// Create a new, unsafe certificate that checks stake, but does not verify authority
    /// signatures.
    pub fn new_unverified(
        committee: &Committee,
        header: Header,
        votes: Vec<(AuthorityIdentifier, BlsSignature)>,
    ) -> DagResult<Certificate> {
        CertificateV1::new_unverified(committee, header, votes)
    }

    /// Create a new, unsafe certificate without verifying authority signatures or stake.
    pub fn new_unsigned(
        committee: &Committee,
        header: Header,
        votes: Vec<(AuthorityIdentifier, BlsSignature)>,
    ) -> DagResult<Certificate> {
        CertificateV1::new_unsigned(committee, header, votes)
    }

    /// Return the group of authorities that signed this certificate.
    ///
    /// This function requires that certificate was verified against given committee
    pub fn signed_authorities(&self, committee: &Committee) -> Vec<BlsPublicKey> {
        match self {
            Certificate::V1(certificate) => certificate.signed_authorities(committee),
        }
    }

    /// Return the total stake and group of authorities that formed the committee for this
    /// certificate.
    pub fn signed_by(&self, committee: &Committee) -> (Stake, Vec<BlsPublicKey>) {
        match self {
            Certificate::V1(certificate) => certificate.signed_by(committee),
        }
    }

    /// Verify the certificate's authority signatures.
    pub fn verify(
        self,
        committee: &Committee,
        worker_cache: &WorkerCache,
    ) -> DagResult<Certificate> {
        match self {
            Certificate::V1(certificate) => certificate.verify(committee, worker_cache),
        }
    }

    /// The certificate's round.
    pub fn round(&self) -> Round {
        match self {
            Certificate::V1(certificate) => certificate.round(),
        }
    }

    /// The certificate's epoch.
    pub fn epoch(&self) -> Epoch {
        match self {
            Certificate::V1(certificate) => certificate.epoch(),
        }
    }

    /// The author of the certificate.
    pub fn origin(&self) -> AuthorityIdentifier {
        match self {
            Certificate::V1(certificate) => certificate.origin(),
        }
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for Certificate {
    type TypedDigest = CertificateDigest;

    fn digest(&self) -> CertificateDigest {
        match self {
            Certificate::V1(data) => data.digest(),
        }
    }
}

/// API for certificates based on version.
#[enum_dispatch]
pub trait CertificateAPI {
    /// The header for the certificate.
    fn header(&self) -> &Header;

    /// The aggregate signature for the certriciate.
    fn aggregated_signature(&self) -> Option<&BlsAggregateSignatureBytes>;

    /// The bitmap of signed authorities for the certificate.
    ///
    /// This is the aggregate signature of all authorities for the certificate.
    fn signed_authorities(&self) -> &roaring::RoaringBitmap;

    /// The time (sec) when the certificate was created.
    fn created_at(&self) -> &TimestampSec;

    /// TODO: better docs
    /// State of the Signature verification
    fn signature_verification_state(&self) -> &SignatureVerificationState;

    /// TODO: better docs
    /// Set the state of the Signature verification.
    fn set_signature_verification_state(&mut self, state: SignatureVerificationState);

    /// Change the certificate's header.
    ///
    /// Only Used for testing.
    #[cfg(any(test, feature = "test-utils"))]
    fn update_header(&mut self, header: Header);

    /// Return a mutable reference to the header.
    ///
    /// Only Used for testing.
    #[cfg(any(test, feature = "test-utils"))]
    fn header_mut(&mut self) -> &mut Header;

    /// Change the certificate's created_at timestamp.
    ///
    /// Only Used for testing.
    #[cfg(any(test, feature = "test-utils"))]
    fn update_created_at(&mut self, timestamp: TimestampSec);
}

// Holds BlsAggregateSignatureBytes but with the added layer to specify the
// signatures verification state. This will be used to take advantage of the
// certificate chain that is formed via the DAG by only verifying the
// leaves of the certificate chain when they are fetched from validators
// during catchup.
#[derive(Clone, Serialize, Deserialize, MallocSizeOf, Debug)]
pub enum SignatureVerificationState {
    // This state occurs when the certificate has not yet received a quorum of
    // signatures.
    Unsigned(BlsAggregateSignatureBytes),
    // This state occurs when a certificate has just been received from the network
    // and has not been verified yet.
    Unverified(BlsAggregateSignatureBytes),
    // This state occurs when a certificate was either created locally, received
    // via brodacast, or fetched but was not the parent of another certificate.
    // Therefore this certificate had to be verified directly.
    VerifiedDirectly(BlsAggregateSignatureBytes),
    // This state occurs when the cert was a parent of another fetched certificate
    // that was verified directly, then this certificate is verified indirectly.
    VerifiedIndirectly(BlsAggregateSignatureBytes),
    // This state occurs only for genesis certificates which always has valid
    // signatures bytes but the bytes are garbage so we don't mark them as verified.
    Genesis,
}

impl Default for SignatureVerificationState {
    fn default() -> Self {
        SignatureVerificationState::Unsigned(BlsAggregateSignatureBytes::default())
    }
}

/// The certificate issued after a successful round of consensus.
#[serde_as]
#[derive(Clone, Serialize, Deserialize, Default, Debug, MallocSizeOf)]
pub struct CertificateV1 {
    /// Certificate's header.
    pub header: Header,
    /// Container for [BlsAggregateSignatureBytes].
    pub signature_verification_state: SignatureVerificationState,
    /// Signatures of all authorities?
    #[serde_as(as = "NarwhalBitmap")]
    signed_authorities: roaring::RoaringBitmap,
    /// Timestamp for certificate
    pub created_at: TimestampSec,
}

impl CertificateAPI for CertificateV1 {
    fn header(&self) -> &Header {
        &self.header
    }

    fn aggregated_signature(&self) -> Option<&BlsAggregateSignatureBytes> {
        match &self.signature_verification_state {
            SignatureVerificationState::VerifiedDirectly(bytes)
            | SignatureVerificationState::Unverified(bytes)
            | SignatureVerificationState::VerifiedIndirectly(bytes)
            | SignatureVerificationState::Unsigned(bytes) => Some(bytes),
            SignatureVerificationState::Genesis => None,
        }
    }

    fn signature_verification_state(&self) -> &SignatureVerificationState {
        &self.signature_verification_state
    }

    fn created_at(&self) -> &TimestampSec {
        &self.created_at
    }

    fn set_signature_verification_state(&mut self, state: SignatureVerificationState) {
        self.signature_verification_state = state;
    }

    fn signed_authorities(&self) -> &roaring::RoaringBitmap {
        &self.signed_authorities
    }

    // Used for testing.

    #[cfg(any(test, feature = "test-utils"))]
    fn update_header(&mut self, header: Header) {
        self.header = header;
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn header_mut(&mut self) -> &mut Header {
        &mut self.header
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn update_created_at(&mut self, timestamp: TimestampSec) {
        self.created_at = timestamp;
    }
}

impl CertificateV1 {
    /// Create a genesis certificate with empty payload.
    pub fn genesis(committee: &Committee) -> Vec<Self> {
        committee
            .authorities()
            .map(|authority| Self {
                header: Header::V1(HeaderV1 {
                    author: authority.id(),
                    epoch: committee.epoch(),
                    ..Default::default()
                }),
                ..Self::default()
            })
            .collect()
    }

    /// Create a new, unsafe certificate that checks stake.
    pub fn new_unverified(
        committee: &Committee,
        header: Header,
        votes: Vec<(AuthorityIdentifier, BlsSignature)>,
    ) -> DagResult<Certificate> {
        Self::new_unsafe(committee, header, votes, true)
    }

    /// Create a new, unsafe certificate that does not check stake.
    pub fn new_unsigned(
        committee: &Committee,
        header: Header,
        votes: Vec<(AuthorityIdentifier, BlsSignature)>,
    ) -> DagResult<Certificate> {
        Self::new_unsafe(committee, header, votes, false)
    }

    /// Create a new certificate without verifying authority signatures.
    fn new_unsafe(
        committee: &Committee,
        header: Header,
        votes: Vec<(AuthorityIdentifier, BlsSignature)>,
        check_stake: bool,
    ) -> DagResult<Certificate> {
        let mut votes = votes;
        votes.sort_by_key(|(pk, _)| *pk);
        let mut votes: VecDeque<_> = votes.into_iter().collect();

        let mut weight = 0;
        let mut sigs = Vec::new();

        let filtered_votes = committee
            .authorities()
            .enumerate()
            .filter(|(_, authority)| {
                if !votes.is_empty() && authority.id() == votes.front().unwrap().0 {
                    sigs.push(votes.pop_front().unwrap());
                    weight += authority.stake();
                    // If there are repeats, also remove them
                    while !votes.is_empty() && votes.front().unwrap() == sigs.last().unwrap() {
                        votes.pop_front().unwrap();
                    }
                    return true;
                }
                false
            })
            .map(|(index, _)| index as u32);

        let signed_authorities= roaring::RoaringBitmap::from_sorted_iter(filtered_votes)
            .map_err(|_| DagError::InvalidBitmap("Failed to convert votes into a bitmap of authority keys. Something is likely very wrong...".to_string()))?;

        // Ensure that all authorities in the set of votes are known
        ensure!(votes.is_empty(), DagError::UnknownAuthority(votes.front().unwrap().0.to_string()));

        // Ensure that the authorities have enough weight
        ensure!(
            !check_stake || weight >= committee.quorum_threshold(),
            DagError::CertificateRequiresQuorum
        );

        let aggregated_signature = if sigs.is_empty() {
            BlsAggregateSignature::default()
        } else {
            BlsAggregateSignature::aggregate::<BlsSignature, Vec<&BlsSignature>>(
                sigs.iter().map(|(_, sig)| sig).collect(),
            )
            .map_err(|_| DagError::InvalidSignature)?
        };

        let aggregate_signature_bytes = BlsAggregateSignatureBytes::from(&aggregated_signature);

        let signature_verification_state = if !check_stake {
            SignatureVerificationState::Unsigned(aggregate_signature_bytes)
        } else {
            SignatureVerificationState::Unverified(aggregate_signature_bytes)
        };

        Ok(Certificate::V1(CertificateV1 {
            header,
            signature_verification_state,
            signed_authorities,
            created_at: now(),
        }))
    }

    /// Return the group of authorities that signed this certificate.
    ///
    /// This function requires that certificate was verified against given committee
    pub fn signed_authorities(&self, committee: &Committee) -> Vec<BlsPublicKey> {
        assert_eq!(committee.epoch(), self.epoch());
        let (_stake, pks) = self.signed_by(committee);
        pks
    }

    /// Return the total stake and group of authorities that formed the committee for this
    /// certificate.
    pub fn signed_by(&self, committee: &Committee) -> (Stake, Vec<BlsPublicKey>) {
        // Ensure the certificate has a quorum.
        let mut weight = 0;

        let auth_indexes = self.signed_authorities.iter().collect::<Vec<_>>();
        let mut auth_iter = 0;
        let pks = committee
            .authorities()
            .enumerate()
            .filter(|(i, authority)| match auth_indexes.get(auth_iter) {
                Some(index) if *index == *i as u32 => {
                    weight += authority.stake();
                    auth_iter += 1;
                    true
                }
                _ => false,
            })
            .map(|(_, authority)| authority.protocol_key().clone())
            .collect();
        (weight, pks)
    }

    /// Verifies the validity of the certificate.
    pub fn verify(
        self,
        committee: &Committee,
        worker_cache: &WorkerCache,
    ) -> DagResult<Certificate> {
        // Ensure the header is from the correct epoch.
        ensure!(
            self.epoch() == committee.epoch(),
            DagError::InvalidEpoch { expected: committee.epoch(), received: self.epoch() }
        );

        // Genesis certificates are always valid.
        if self.round() == 0 && Self::genesis(committee).contains(&self) {
            return Ok(Certificate::V1(self));
        }

        // Save signature verifications when the header is invalid.
        self.header.validate(committee, worker_cache)?;

        let (weight, pks) = self.signed_by(committee);

        ensure!(weight >= committee.quorum_threshold(), DagError::CertificateRequiresQuorum);

        let verified_cert = self.verify_signature(pks)?;

        Ok(verified_cert)
    }

    fn verify_signature(mut self, pks: Vec<BlsPublicKey>) -> DagResult<Certificate> {
        let aggregrate_signature_bytes = match self.signature_verification_state {
            SignatureVerificationState::VerifiedIndirectly(_)
            | SignatureVerificationState::VerifiedDirectly(_)
            | SignatureVerificationState::Genesis => return Ok(Certificate::V1(self)),
            SignatureVerificationState::Unverified(ref bytes) => bytes,
            SignatureVerificationState::Unsigned(_) => {
                return Err(DagError::CertificateRequiresQuorum);
            }
        };

        // Verify the signatures
        let certificate_digest: Digest<{ crypto::DIGEST_LENGTH }> = Digest::from(self.digest());
        BlsAggregateSignature::try_from(aggregrate_signature_bytes)
            .map_err(|_| DagError::InvalidSignature)?
            .verify_secure(&to_intent_message(certificate_digest), &pks[..])
            .map_err(|_| DagError::InvalidSignature)?;

        self.signature_verification_state =
            SignatureVerificationState::VerifiedDirectly(aggregrate_signature_bytes.clone());

        Ok(Certificate::V1(self))
    }

    /// The certificate's round.
    pub fn round(&self) -> Round {
        self.header.round()
    }

    /// The certificate's epoch.
    pub fn epoch(&self) -> Epoch {
        self.header.epoch()
    }

    /// The author of the certificate.
    pub fn origin(&self) -> AuthorityIdentifier {
        self.header.author()
    }
}

// Certificate version is validated against network protocol version. If CertificateV1
// is being used then the cert will also be marked as Unverifed as this certificate
// is assumed to be received from the network. This SignatureVerificationState is
// why the modified certificate is being returned.
pub fn validate_received_certificate_version(
    mut certificate: Certificate,
) -> eyre::Result<Certificate> {
    match certificate {
        Certificate::V1(_) => {
            // CertificateV1 was received from the network so we need to mark
            // certificate aggregated signature state as unverified.
            certificate.set_signature_verification_state(SignatureVerificationState::Unverified(
                certificate.aggregated_signature().ok_or(eyre::eyre!("Invalid signature"))?.clone(),
            ));
        }
    };
    Ok(certificate)
}

/// Certificate digest.
#[derive(
    Clone, Copy, Serialize, Deserialize, Default, MallocSizeOf, PartialEq, Eq, Hash, PartialOrd, Ord,
)]
pub struct CertificateDigest([u8; crypto::DIGEST_LENGTH]);

impl CertificateDigest {
    /// Create a new instance of CertificateDigest.
    pub fn new(digest: [u8; crypto::DIGEST_LENGTH]) -> Self {
        CertificateDigest(digest)
    }
}

impl AsRef<[u8]> for CertificateDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<CertificateDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(hd: CertificateDigest) -> Self {
        Digest::new(hd.0)
    }
}

impl fmt::Debug for CertificateDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", general_purpose::STANDARD.encode(self.0))
    }
}

impl fmt::Display for CertificateDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", general_purpose::STANDARD.encode(self.0).get(0..16).ok_or(fmt::Error)?)
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for CertificateV1 {
    type TypedDigest = CertificateDigest;

    fn digest(&self) -> CertificateDigest {
        CertificateDigest(self.header.digest().0)
    }
}

impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Certificate::V1(data) => write!(
                f,
                "{}: C{}({}, {}, E{})",
                data.digest(),
                data.round(),
                data.origin(),
                data.header.digest(),
                data.epoch()
            ),
        }
    }
}

impl PartialEq for Certificate {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Certificate::V1(data), Certificate::V1(other_data)) => data.eq(other_data),
        }
    }
}

impl PartialEq for CertificateV1 {
    fn eq(&self, other: &Self) -> bool {
        let mut ret = self.header().digest() == other.header().digest();
        ret &= self.round() == other.round();
        ret &= self.epoch() == other.epoch();
        ret &= self.origin() == other.origin();
        ret
    }
}
