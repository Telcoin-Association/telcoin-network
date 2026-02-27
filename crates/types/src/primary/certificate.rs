//! Certificates and their digests.
//!
//! Certificates are issued by Primaries once their proposed headers are verified by a quorum (2f+1)
//! of peers.

use crate::{
    committee::quorum_threshold,
    crypto::{
        self, to_intent_message, BlsAggregateSignature, BlsPublicKey, BlsSignature,
        ValidatorAggregateSignature,
    },
    ensure,
    error::{CertificateError, CertificateResult, DagError, DagResult, HeaderError},
    now,
    serde::RoaringBitmapSerde,
    Authority, AuthorityIdentifier, BlockHash, Committee, Digest, Epoch, Hash, Header, Round,
    TimestampSec, VotingPower,
};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

/// Certificates are the output of consensus.
/// The certificate issued after a successful round of consensus.
#[serde_as]
#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Certificate {
    /// Certificate's header.
    pub header: Header,
    /// Container for [BlsAggregateSignatureBytes].
    pub signature_verification_state: SignatureVerificationState,
    /// Bitmap that indicates which authorities from committee signed this certificate.
    #[serde_as(as = "RoaringBitmapSerde")]
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
            .iter()
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
        Self::new_internal(
            committee,
            header,
            votes
                .into_iter()
                .filter_map(|(a, sig)| committee.authority(&a).map(|a| (*a.protocol_key(), sig)))
                .collect(),
            true,
        )
    }

    /// Create a new, unsafe certificate that does not check stake.
    pub fn new_unsigned_for_test(
        committee: &Committee,
        header: Header,
        votes: Vec<(AuthorityIdentifier, BlsSignature)>,
    ) -> DagResult<Certificate> {
        Self::new_internal(
            committee,
            header,
            votes
                .into_iter()
                .filter_map(|(a, sig)| committee.authority(&a).map(|a| (*a.protocol_key(), sig)))
                .collect(),
            false,
        )
    }

    /// Create a new certificate without verifying authority signatures.
    fn new_internal(
        committee: &Committee,
        header: Header,
        // We need votes to be a BTreeMap to force authorities to be in the expected order.
        mut votes: BTreeMap<BlsPublicKey, BlsSignature>,
        check_stake: bool,
    ) -> DagResult<Certificate> {
        let mut weight = 0;
        let mut sigs = Vec::new();

        let auths: BTreeMap<BlsPublicKey, Authority> =
            committee.authorities().into_iter().map(|a| (*a.protocol_key(), a)).collect();
        let filtered_votes = auths
            .iter()
            .enumerate()
            .filter(|(_, (key, authority))| {
                if !votes.is_empty() && *key == votes.first_key_value().expect("votes not empty").0
                {
                    sigs.push(votes.pop_first().expect("votes not empty"));
                    weight += authority.voting_power();
                    let sig_last = sigs.last().expect("sigs not empty");
                    // If there are repeats, also remove them
                    while !votes.is_empty()
                        && votes.first_key_value().expect("votes not empty")
                            == (&sig_last.0, &sig_last.1)
                    {
                        votes.pop_first().expect("votes not empty");
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
            DagError::UnknownAuthority(
                votes.first_key_value().expect("votes not empty").0.to_string()
            )
        );

        // Ensure that the authorities have enough weight
        ensure!(
            !check_stake || weight >= committee.quorum_threshold(),
            DagError::CertificateRequiresQuorum
        );

        let sigs: Vec<BlsSignature> = sigs.iter().map(|(_, sig)| *sig).collect();
        let bls_signature = if sigs.is_empty() {
            BlsSignature::default()
        } else {
            let aggregated_signature = BlsAggregateSignature::aggregate(&sigs[..], true)
                .map_err(|_| DagError::InvalidSignature)?;

            aggregated_signature.to_signature()
        };

        let signature_verification_state = if !check_stake {
            SignatureVerificationState::Unsigned(bls_signature)
        } else {
            SignatureVerificationState::Unverified(bls_signature)
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
        let (_stake, pks) = self.signed_by(&committee.bls_keys());
        pks
    }

    /// Return the total stake and group of authorities that formed the committee for this
    /// certificate.
    pub fn signed_by(
        &self,
        committee: &BTreeSet<BlsPublicKey>,
    ) -> (VotingPower, Vec<BlsPublicKey>) {
        // Ensure the certificate has a quorum.
        let mut weight = 0;

        let auth_indexes = self.signed_authorities.iter().collect::<Vec<_>>();
        let mut auth_iter = 0;
        let pks = committee
            .iter()
            .enumerate()
            .filter_map(|(i, key)| match auth_indexes.get(auth_iter) {
                Some(index) if *index == i as u32 => {
                    weight += 1; // All validators have a voting weight of 1.
                    auth_iter += 1;
                    Some(*key)
                }
                _ => None,
            })
            .collect();
        (weight, pks)
    }

    /// Validate the certificate and verify signatures against the committee.
    ///
    /// The method updates the certificate with verified signature state.
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
    pub fn validate_and_verify(&mut self, committee: &Committee) -> CertificateResult<()> {
        // ensure the header is from the correct epoch
        ensure!(
            self.epoch() == committee.epoch(),
            CertificateError::from(HeaderError::InvalidEpoch {
                theirs: self.epoch(),
                ours: committee.epoch()
            })
        );

        // Genesis certificates are always valid.
        if self.round() == 0 && Self::genesis(committee).contains(self) {
            return Ok(());
        }

        // Save signature verifications when the header is invalid.
        self.header.validate(committee)?;

        let (weight, pks) = self.signed_by(&committee.bls_keys());

        let threshold = committee.quorum_threshold();
        ensure!(weight >= threshold, CertificateError::Inquorate { stake: weight, threshold });

        self.verify_signature(pks)?;

        Ok(())
    }

    /// Performs a signature verification of a certificate against committee.
    /// Will clear the state first and revalidate even if it appears to be valid.
    pub fn verify_cert(&mut self, committee: &BTreeSet<BlsPublicKey>) -> CertificateResult<()> {
        self.validate_received()?;
        let (weight, pks) = self.signed_by(committee);

        // All validator have a vote weight of 1.
        let threshold = quorum_threshold(committee.len() as u64);
        ensure!(weight >= threshold, CertificateError::Inquorate { stake: weight, threshold });

        self.verify_signature(pks)?;

        Ok(())
    }

    /// Check the verification state and try to verify directly.
    fn verify_signature(&mut self, pks: Vec<BlsPublicKey>) -> CertificateResult<()> {
        // get signature from verification state
        let signature = match self.signature_verification_state {
            SignatureVerificationState::VerifiedIndirectly(_)
            | SignatureVerificationState::VerifiedDirectly(_)
            | SignatureVerificationState::Genesis => return Ok(()),
            SignatureVerificationState::Unverified(ref sig) => sig,
            SignatureVerificationState::Unsigned(_) => {
                return Err(CertificateError::Unsigned);
            }
        };

        // Verify the signatures
        let certificate_digest = self.digest();
        let aggregate_signature = BlsAggregateSignature::from_signature(signature);
        if !aggregate_signature.verify_secure(&to_intent_message(certificate_digest), &pks[..]) {
            return Err(CertificateError::InvalidSignature);
        }

        self.signature_verification_state =
            SignatureVerificationState::VerifiedDirectly(*signature);

        Ok(())
    }

    /// Validate certificate was received and ready for verification.
    pub fn validate_received(&mut self) -> CertificateResult<()> {
        self.set_signature_verification_state(SignatureVerificationState::Unverified(
            self.aggregated_signature()
                .ok_or(CertificateError::RecoverBlsAggregateSignatureBytes)?,
        ));
        Ok(())
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
    pub fn origin(&self) -> &AuthorityIdentifier {
        self.header.author()
    }

    /// The header for the certificate.
    pub fn header(&self) -> &Header {
        &self.header
    }

    /// The aggregate signature for the certriciate.
    pub fn aggregated_signature(&self) -> Option<BlsSignature> {
        match &self.signature_verification_state {
            SignatureVerificationState::VerifiedDirectly(sig)
            | SignatureVerificationState::Unverified(sig)
            | SignatureVerificationState::VerifiedIndirectly(sig)
            | SignatureVerificationState::Unsigned(sig) => Some(*sig),
            SignatureVerificationState::Genesis => None,
        }
    }

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
#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum SignatureVerificationState {
    /// This state occurs when the certificate has not yet received a quorum of
    /// signatures.
    Unsigned(BlsSignature),
    /// This state occurs when a certificate has just been received from the network
    /// and has not been verified yet.
    Unverified(BlsSignature),
    /// This state occurs when a certificate was either created locally, received
    /// via brodacast, or fetched but was not the parent of another certificate.
    /// Therefore this certificate had to be verified directly.
    VerifiedDirectly(BlsSignature),
    /// This state occurs when the cert was a parent of another fetched certificate
    /// that was verified directly, then this certificate is verified indirectly.
    VerifiedIndirectly(BlsSignature),
    /// This state occurs only for genesis certificates which always has valid
    /// signatures bytes but the bytes are garbage so we don't mark them as verified.
    Genesis,
}

impl Default for SignatureVerificationState {
    fn default() -> Self {
        SignatureVerificationState::Unsigned(BlsSignature::default())
    }
}

/// Process certificate received by setting the verification state.
///
/// Recover signature bytes from the aggregated signature and set the signature verification state
/// to unverified. This rejects genesis certificates (only called when certificate is fetched from
/// peer).
pub fn validate_fetched_certificate(
    mut certificate: Certificate,
) -> CertificateResult<Certificate> {
    certificate.set_signature_verification_state(SignatureVerificationState::Unverified(
        certificate
            .aggregated_signature()
            .ok_or(CertificateError::RecoverBlsAggregateSignatureBytes)?,
    ));
    Ok(certificate)
}

/// Certificate digest.
#[derive(
    Clone, Copy, Default, PartialEq, Eq, std::hash::Hash, PartialOrd, Ord, Serialize, Deserialize,
)]
pub struct CertificateDigest(Digest<{ crypto::DIGEST_LENGTH }>);

impl CertificateDigest {
    /// Create a new instance of CertificateDigest.
    pub fn new(digest: [u8; crypto::DIGEST_LENGTH]) -> Self {
        CertificateDigest(Digest { digest })
    }
}

impl AsRef<[u8]> for CertificateDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0.digest
    }
}

impl From<CertificateDigest> for Digest<{ crypto::DIGEST_LENGTH }> {
    fn from(hd: CertificateDigest) -> Self {
        hd.0
    }
}

impl fmt::Debug for CertificateDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for CertificateDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0.to_string().get(0..16).ok_or(fmt::Error)?)
    }
}

impl Hash<{ crypto::DIGEST_LENGTH }> for Certificate {
    type TypedDigest = CertificateDigest;

    fn digest(&self) -> CertificateDigest {
        CertificateDigest(Digest { digest: self.header.digest().into() })
    }
}

impl From<CertificateDigest> for BlockHash {
    fn from(value: CertificateDigest) -> Self {
        Self::from(value.0.digest)
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
