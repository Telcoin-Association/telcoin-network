//! Certificates and their digests.
//!
//! Certificates are issued by Primaries once their proposed headers are verified by a quorum (2f+1)
//! of peers.

use crate::{
    crypto::{
        self, to_intent_message, BlsAggregateSignature, BlsAggregateSignatureBytes, BlsPublicKey,
        BlsSignature, ValidatorAggregateSignature,
    },
    ensure,
    error::{CertificateError, CertificateResult, DagError, DagResult, HeaderError},
    now,
    serde::CertificateSignatures,
    AuthorityIdentifier, BlockHash, Committee, Epoch, Header, Round, Stake, TimestampSec,
    WorkerCache,
};
use base64::{engine::general_purpose, Engine};
use fastcrypto::{
    hash::{Digest, Hash},
    traits::AggregateAuthenticator,
};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::{collections::VecDeque, fmt};

/// Certificates are the output of consensus.
/// The certificate issued after a successful round of consensus.
#[serde_as]
#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Certificate {
    /// Certificate's header.
    pub header: Header,
    /// Container for [BlsAggregateSignatureBytes].
    pub signature_verification_state: SignatureVerificationState,
    /// Signatures of all authorities
    #[serde_as(as = "CertificateSignatures")]
    signed_authorities: roaring::RoaringBitmap,
    /// Timestamp for certificate creation.
    ///
    /// This is only used for performance metrics. Consensus relies on the header's timestamp.
    created_at: TimestampSec,
}

impl Certificate {
    /// Create a genesis certificate with empty payload.
    pub fn genesis(committee: &Committee) -> Vec<Self> {
        committee
            .authorities()
            .map(|authority| Self {
                header: Header {
                    author: authority.id(),
                    epoch: committee.epoch(),
                    ..Default::default()
                },
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
        mut votes: Vec<(AuthorityIdentifier, BlsSignature)>,
        check_stake: bool,
    ) -> DagResult<Certificate> {
        votes.sort_by_key(|(pk, _)| *pk);
        let mut votes: VecDeque<_> = votes.into_iter().collect();

        let mut weight = 0;
        let mut sigs = Vec::new();

        let filtered_votes = committee
            .authorities()
            .enumerate()
            .filter(|(_, authority)| {
                if !votes.is_empty() && authority.id() == votes.front().expect("votes not empty").0
                {
                    sigs.push(votes.pop_front().expect("votes not empty"));
                    weight += authority.stake();
                    // If there are repeats, also remove them
                    while !votes.is_empty()
                        && votes.front().expect("votes not empty")
                            == sigs.last().expect("votes not empty")
                    {
                        votes.pop_front().expect("votes not empty");
                    }
                    return true;
                }
                false
            })
            .map(|(index, _)| index as u32);

        let signed_authorities= roaring::RoaringBitmap::from_sorted_iter(filtered_votes)
            .map_err(|_| DagError::InvalidBitmap("Failed to convert votes into a bitmap of authority keys. Something is likely very wrong...".to_string()))?;

        // Ensure that all authorities in the set of votes are known
        ensure!(
            votes.is_empty(),
            DagError::UnknownAuthority(votes.front().expect("votes not empty").0.to_string())
        );

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

        Ok(Certificate {
            header,
            signature_verification_state,
            signed_authorities,
            created_at: now(),
        })
    }

    /// Return the group of authorities that signed this certificate.
    ///
    /// This function requires that certificate was verified against given committee
    pub fn signed_authorities_with_committee(&self, committee: &Committee) -> Vec<BlsPublicKey> {
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

    /// Validate the certificate and verify signatures against the committee.
    ///
    /// The method returns a certificate with verified signature state.
    ///
    /// [SignatureVerificationState] stores both the verification status and signature bytes
    /// together. While this creates some data redundancy with signed_authorities, keeping them
    /// coupled provides important benefits:
    ///
    /// 1. Atomic State Updates - Changes to verification status are guaranteed to reference the
    ///    exact signature bytes that were verified. This prevents state/signature mismatches that
    ///    could occur if stored separately.
    ///
    /// 2. Verification Integrity - The verification status can only transition while operating on
    ///    the specific signature bytes that were validated. This maintains a clear chain of trust
    ///    through the verification process.
    ///
    /// 3. Invariant Preservation - Makes it impossible to have invalid states like:
    ///    - VerifiedDirectly status with different signature bytes than what was actually verified
    ///    - An Unsigned state containing signature bytes
    ///    - A Verified state with missing/corrupted signature bytes
    ///
    /// While storing signatures in both places uses more memory, the strong correctness guarantees
    /// outweigh the storage cost for certificate verification where maintaining cryptographic
    /// integrity is critical.
    pub fn verify(
        self,
        committee: &Committee,
        worker_cache: &WorkerCache,
    ) -> CertificateResult<Certificate> {
        // ensure the header is from the correct epoch
        ensure!(
            self.epoch() == committee.epoch(),
            CertificateError::from(HeaderError::InvalidEpoch {
                theirs: self.epoch(),
                ours: committee.epoch()
            })
        );

        // Genesis certificates are always valid.
        if self.round() == 0 && Self::genesis(committee).contains(&self) {
            return Ok(self);
        }

        // Save signature verifications when the header is invalid.
        self.header.validate(committee, worker_cache)?;

        let (weight, pks) = self.signed_by(committee);

        let threshold = committee.quorum_threshold();
        ensure!(weight >= threshold, CertificateError::Inquorate { stake: weight, threshold });

        let verified_cert = self.verify_signature(pks)?;

        Ok(verified_cert)
    }

    /// Performs a signature verification of a certificate against committee.
    /// Will clear the state first and revalidate even if it appears to be valid.
    pub fn verify_cert(mut self, committee: &Committee) -> CertificateResult<Certificate> {
        self = self.validate_received()?;
        let (weight, pks) = self.signed_by(committee);

        let threshold = committee.quorum_threshold();
        ensure!(weight >= threshold, CertificateError::Inquorate { stake: weight, threshold });

        let verified_cert = self.verify_signature(pks)?;

        Ok(verified_cert)
    }

    /// Check the verification state and try to verify directly.
    fn verify_signature(mut self, pks: Vec<BlsPublicKey>) -> CertificateResult<Certificate> {
        // get signature from verification state
        let aggregrate_signature_bytes = match self.signature_verification_state {
            SignatureVerificationState::VerifiedIndirectly(_)
            | SignatureVerificationState::VerifiedDirectly(_)
            | SignatureVerificationState::Genesis => return Ok(self),
            SignatureVerificationState::Unverified(ref bytes) => bytes,
            SignatureVerificationState::Unsigned(_) => {
                return Err(CertificateError::Unsigned);
            }
        };

        // Verify the signatures
        let certificate_digest = self.digest();
        BlsAggregateSignature::try_from(aggregrate_signature_bytes)?
            .verify_secure(&to_intent_message(certificate_digest), &pks[..])?;

        self.signature_verification_state =
            SignatureVerificationState::VerifiedDirectly(aggregrate_signature_bytes.clone());

        Ok(self)
    }

    /// Validate certificate was received and ready for verification.
    pub fn validate_received(mut self) -> CertificateResult<Self> {
        self.set_signature_verification_state(SignatureVerificationState::Unverified(
            self.aggregated_signature()
                .ok_or(CertificateError::RecoverBlsAggregateSignatureBytes)?
                .clone(),
        ));
        Ok(self)
    }

    /// The certificate's round.
    pub fn round(&self) -> Round {
        self.header.round()
    }

    /// The certificate's epoch.
    pub fn epoch(&self) -> Epoch {
        self.header.epoch()
    }

    /// The nonce this certificate.
    pub fn nonce(&self) -> u64 {
        self.header.nonce()
    }

    /// The author of the certificate.
    pub fn origin(&self) -> AuthorityIdentifier {
        self.header.author()
    }

    /// The header for the certificate.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// The aggregate signature for the certriciate.
    pub fn aggregated_signature(&self) -> Option<&BlsAggregateSignatureBytes> {
        match &self.signature_verification_state {
            SignatureVerificationState::VerifiedDirectly(bytes)
            | SignatureVerificationState::Unverified(bytes)
            | SignatureVerificationState::VerifiedIndirectly(bytes)
            | SignatureVerificationState::Unsigned(bytes) => Some(bytes),
            SignatureVerificationState::Genesis => None,
        }
    }

    /// TODO: better docs
    /// State of the Signature verification
    pub fn signature_verification_state(&self) -> &SignatureVerificationState {
        &self.signature_verification_state
    }

    /// The time (sec) when the certificate was created.
    ///
    /// This is only used for performance metrics. Consensus relies on the header's timestamp.
    pub fn created_at(&self) -> &TimestampSec {
        &self.created_at
    }

    /// TODO: better docs
    /// Set the state of the Signature verification.
    pub fn set_signature_verification_state(&mut self, state: SignatureVerificationState) {
        self.signature_verification_state = state;
    }

    /// The bitmap of signed authorities for the certificate.
    ///
    /// This is the aggregate signature of all authorities for the certificate.
    pub fn signed_authorities(&self) -> &roaring::RoaringBitmap {
        &self.signed_authorities
    }

    /// Helper method if the certificate's verification state is verified.
    pub fn is_verified(&self) -> bool {
        matches!(
            self.signature_verification_state,
            SignatureVerificationState::VerifiedDirectly(_)
                | SignatureVerificationState::VerifiedIndirectly(_)
                | SignatureVerificationState::Genesis
        )
    }

    // Used for testing.

    /// Change the certificate's header.
    ///
    /// Only Used for testing.
    pub fn update_header_for_test(&mut self, header: Header) {
        self.header = header;
    }

    /// Return a mutable reference to the header.
    ///
    /// Only Used for testing.
    pub fn header_mut_for_test(&mut self) -> &mut Header {
        &mut self.header
    }

    /// Change the certificate's created_at timestamp.
    ///
    /// Only Used for testing.
    pub fn update_created_at_for_test(&mut self, timestamp: TimestampSec) {
        self.created_at = timestamp;
    }
}

impl From<&[u8]> for Certificate {
    fn from(value: &[u8]) -> Self {
        crate::decode(value)
    }
}

impl From<&Certificate> for Vec<u8> {
    fn from(value: &Certificate) -> Self {
        crate::encode(value)
    }
}

// This will be used to take advantage of the
// certificate chain that is formed via the DAG by only verifying the
// leaves of the certificate chain when they are fetched from validators
// during catchup.
/// SignatureVerificationState stores both the verification status and signature bytes together.
/// While this creates some data redundancy with signed_authorities, keeping them coupled provides
/// important benefits:
/// - atomic state updates: changes to verification status are guaranteed to reference the exact
///   signature bytes that were verified. this prevents state/signature mismatches that could occur
///   if stored separately.
///
/// - verification integrity: the verification status can only transition while operating on the
///   specific signature bytes that were validated. this maintains a clear chain of trust through
///   the verification process.
///
/// - impossible to have invalid states like:
///    - `VerifiedDirectly` status with different signature bytes than what was actually verified
///    - an unsigned state containing signature bytes
///    - a verified state with missing/corrupted signature bytes
#[derive(Clone, Serialize, Deserialize, Debug)]
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

/// Process certificate received by setting the verification state.
///
/// Recover signature bytes from the aggregated signature and set the signature verification state
/// to unverified.
pub fn validate_received_certificate(
    mut certificate: Certificate,
) -> CertificateResult<Certificate> {
    certificate.set_signature_verification_state(SignatureVerificationState::Unverified(
        certificate
            .aggregated_signature()
            .ok_or(CertificateError::RecoverBlsAggregateSignatureBytes)?
            .clone(),
    ));
    Ok(certificate)
}

/// Certificate digest.
#[derive(Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

impl Hash<{ crypto::DIGEST_LENGTH }> for Certificate {
    type TypedDigest = CertificateDigest;

    fn digest(&self) -> CertificateDigest {
        CertificateDigest(self.header.digest().0)
    }
}

impl From<CertificateDigest> for BlockHash {
    fn from(value: CertificateDigest) -> Self {
        Self::from(value.0)
    }
}

impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: C{}({}, {}, E{})",
            self.digest(),
            self.round(),
            self.origin(),
            self.header.digest(),
            self.epoch()
        )
    }
}

impl PartialEq for Certificate {
    fn eq(&self, other: &Self) -> bool {
        let mut ret = self.header().digest() == other.header().digest();
        ret &= self.round() == other.round();
        ret &= self.epoch() == other.epoch();
        ret &= self.origin() == other.origin();
        ret
    }
}
