//! TNCodec tests used by the consensus network libp2p req/res protocol.

use super::*;
use crate::{
    common::{TestPrimaryRequest, TestPrimaryResponse},
    TNCodec,
};
use libp2p::{Multiaddr, StreamProtocol};
use rand::{rngs::StdRng, SeedableRng};
use std::collections::{HashMap, HashSet};
use tn_types::{BlsKeypair, Certificate, Header, HeaderDigest, NetworkKeypair, NetworkPublicKey};

#[tokio::test]
async fn test_encode_decode_same_message() {
    let max_chunk_size = 1024 * 1024; // 1mb
    let mut codec = TNCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(max_chunk_size);
    let protocol = StreamProtocol::new("/tn-test");

    // encode request
    let mut encoded = Vec::new();
    let request = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default()],
    };
    codec
        .write_request(&protocol, &mut encoded, request.clone())
        .await
        .expect("write valid request");

    // now decode request
    let decoded =
        codec.read_request(&protocol, &mut encoded.as_ref()).await.expect("read valid request");
    assert_eq!(decoded, request);

    // encode response
    let mut encoded = Vec::new();
    let response = TestPrimaryResponse::MissingParents(vec![HeaderDigest::new([b'a'; 32])]);
    codec
        .write_response(&protocol, &mut encoded, response.clone())
        .await
        .expect("write valid response");

    // now decode response
    let decoded =
        codec.read_response(&protocol, &mut encoded.as_ref()).await.expect("read valid response");
    assert_eq!(decoded, response);
}

/// The dedicated peer-exchange goodbye codec round-trips a bare [`PeerExchangeMap`]
/// in both directions: the request carries the disconnecting node's exchange map
/// and the response carries the (empty today) ack.
#[tokio::test]
async fn test_peer_exchange_codec_round_trip() {
    let max_chunk_size = 1024 * 1024; // 1mb
    let mut codec = PeerExchangeCodec::new(max_chunk_size);
    let protocol = StreamProtocol::new("/tn-test-peer-exchange");

    // a representative goodbye payload
    let mut rng = StdRng::from_seed([0; 32]);
    let bls = *BlsKeypair::generate(&mut rng).public();
    let net: NetworkPublicKey = NetworkKeypair::generate_ed25519().public().clone().into();
    let multiaddr: Multiaddr = "/ip4/127.0.0.1/udp/38300/quic-v1".parse().expect("valid multiaddr");
    let exchange = PeerExchangeMap::from(HashMap::from([(bls, (net, HashSet::from([multiaddr])))]));

    // encode and decode the exchange as the request
    let mut encoded = Vec::new();
    codec
        .write_request(&protocol, &mut encoded, exchange.clone())
        .await
        .expect("write valid peer exchange request");
    let decoded = codec
        .read_request(&protocol, &mut encoded.as_ref())
        .await
        .expect("read valid peer exchange request");
    assert_eq!(decoded, exchange);

    // the empty ack round-trips as the response
    let mut encoded = Vec::new();
    let ack = PeerExchangeMap::default();
    codec
        .write_response(&protocol, &mut encoded, ack.clone())
        .await
        .expect("write valid peer exchange ack");
    let decoded = codec
        .read_response(&protocol, &mut encoded.as_ref())
        .await
        .expect("read valid peer exchange ack");
    assert_eq!(decoded, ack);
}

#[tokio::test]
async fn test_fail_to_write_message_too_big() {
    let max_chunk_size = 100; // 100 bytes is too small
    let mut codec = TNCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(max_chunk_size);
    let protocol = StreamProtocol::new("/tn-test");

    // encode request
    let mut encoded = Vec::new();
    let request = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default()],
    };
    let res = codec.write_request(&protocol, &mut encoded, request).await;
    assert!(res.is_err());

    // encode response
    let mut encoded = Vec::new();
    let response = TestPrimaryResponse::MissingCertificates(vec![Certificate::default()]);
    let res = codec.write_response(&protocol, &mut encoded, response).await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_reject_message_prefix_too_big() {
    let max_chunk_size = 344; // 344 bytes
    let mut honest_peer = TNCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(max_chunk_size);
    let protocol = StreamProtocol::new("/tn-test");
    // malicious peer writes legit messages that are too big
    // "legit" means correct prefix and valid data. the only problem is message too big for
    // receiving peer
    let mut malicious_peer = TNCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(1024 * 1024);

    //
    // test requests first
    //
    // sanity check
    let mut encoded = Vec::new();

    //println!("size: {}", std::mem::size_of::<TestPrimaryRequest>());
    // this is 344 bytes uncompressed (max chunk size)
    let request = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default()],
    };
    malicious_peer
        .write_request(&protocol, &mut encoded, request.clone())
        .await
        .expect("write legit and valid request");
    let decoded = honest_peer
        .read_request(&protocol, &mut encoded.as_ref())
        .await
        .expect("read valid request");
    assert_eq!(decoded, request);

    // now encode legit message that's too big for honest peer
    let mut encoded = Vec::new();
    // this is 344 bytes uncompressed
    let big_request = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default(), Certificate::default()],
    };
    malicious_peer
        .write_request(&protocol, &mut encoded, big_request)
        .await
        .expect("write legit request");
    // prefix length should cause error
    let res = honest_peer.read_request(&protocol, &mut encoded.as_ref()).await;
    assert!(res.is_err());

    //
    // test the same for responses
    //
    // sanity check that block within bounds works
    let mut encoded = Vec::new();
    // 138 bytes uncompressed
    let response = TestPrimaryResponse::MissingCertificates(vec![Certificate::default()]);
    malicious_peer
        .write_response(&protocol, &mut encoded, response.clone())
        .await
        .expect("write legit and valid response");
    let decoded = honest_peer
        .read_response(&protocol, &mut encoded.as_ref())
        .await
        .expect("read valid response");
    assert_eq!(decoded, response);

    // now encode legit message that's too big for honest peer
    let mut encoded = Vec::new();
    // > 416 bytes uncompressed
    let big_response = TestPrimaryResponse::MissingCertificates(vec![
        Certificate::default(),
        Certificate::default(),
        Certificate::default(),
        Certificate::default(),
    ]);
    malicious_peer
        .write_response(&protocol, &mut encoded, big_response)
        .await
        .expect("write legit response");
    // prefix length should cause error
    let res = honest_peer.read_response(&protocol, &mut encoded.as_ref()).await;
    assert!(res.is_err())
}

#[tokio::test]
async fn test_malicious_prefix_deceives_peer_to_read_message_and_fails() {
    let max_chunk_size = 208; // 208 bytes max message size
    let mut honest_peer = TNCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(max_chunk_size);
    let protocol = StreamProtocol::new("/tn-test");
    // malicious peer writes legit messages that are too big
    // "legit" means correct prefix and valid data. the only problem is message too big
    let mut malicious_peer = TNCodec::<TestPrimaryRequest, TestPrimaryResponse>::new(1024 * 1024);

    //
    // test requests first
    //
    // encode valid message that's too big and change prefix to deceive peer into trying to read
    // content
    let mut encoded = Vec::new();
    // this is 344 bytes uncompressed
    // but only 74 bytes compressed (within max size)
    let big_request = TestPrimaryRequest::Vote {
        header: Header::default(),
        parents: vec![Certificate::default(), Certificate::default()],
    };
    malicious_peer
        .write_request(&protocol, &mut encoded, big_request)
        .await
        .expect("write legit request");
    // assert prefix is greater than peer's max chunk size
    let mut actual_prefix = [0; 4];
    actual_prefix.clone_from_slice(&encoded[0..4]);
    let honest_length = u32::from_le_bytes(actual_prefix) as usize;

    // sanity check
    assert!(honest_length > max_chunk_size);
    assert!(encoded.len() < max_chunk_size);

    // manipulate prefix to obfuscate actual message size is too big
    // this sets prefix to the honest peer's max message length,
    // which is considered valid and within message size bounds
    encoded[0..4].clone_from_slice(&100u32.to_le_bytes());

    // should cause an unexpected EOF
    let res = honest_peer.read_request(&protocol, &mut encoded.as_ref()).await;
    assert!(res.is_err());

    //
    // test responses first
    //
    // encode valid message that's too big and change prefix to deceive peer into trying to read
    // content
    let mut encoded = Vec::new();
    // this is 274 bytes uncompressed (more than max)
    // but only 62 bytes compressed (within max size)
    let big_response = TestPrimaryResponse::MissingCertificates(vec![
        Certificate::default(),
        Certificate::default(),
    ]);
    malicious_peer
        .write_response(&protocol, &mut encoded, big_response)
        .await
        .expect("write legit response");
    // assert prefix is greater than peer's max chunk size
    let mut actual_prefix = [0; 4];
    actual_prefix.clone_from_slice(&encoded[0..4]);
    let honest_length = u32::from_le_bytes(actual_prefix) as usize;

    // sanity check
    assert!(honest_length > max_chunk_size);
    assert!(encoded.len() < max_chunk_size);

    // manipulate prefix to obfuscate actual message size is too big
    // this sets prefix to the honest peer's max message length,
    // which is considered valid and within message size bounds
    encoded[0..4].clone_from_slice(&100u32.to_le_bytes());

    // should cause an unexpected EOF
    let res = honest_peer.read_response(&protocol, &mut encoded.as_ref()).await;
    assert!(res.is_err());
}

/// Build the 8-byte length header a malicious peer would send: a 1 MiB uncompressed length (passes
/// the `<= max_message_size` check) paired with a ~1.2 MB compressed length (passes the
/// `<= snap::raw::max_compress_len(1 MiB)` check, which is ~1_223_370).
fn malicious_header(uncompressed_len: u32, compressed_len: u32) -> Vec<u8> {
    let mut header = Vec::with_capacity(8);
    header.extend_from_slice(&uncompressed_len.to_le_bytes());
    header.extend_from_slice(&compressed_len.to_le_bytes());
    header
}

/// Regression test for issue #728: a peer that sends only the 8-byte length header and withholds
/// the body must not cause the decoder to commit memory proportional to the declared prefixes.
#[tokio::test]
async fn test_header_only_does_not_commit_declared_buffers() {
    let max_chunk_size = 1024 * 1024; // 1mb
    let header = malicious_header(1024 * 1024, 1_200_000);
    assert_eq!(header.len(), 8, "attacker input is only the 8-byte header");

    let mut decode_buffer = Vec::new();
    let mut compressed_buffer = Vec::new();
    // reader yields the 8 header bytes then EOF (no body)
    let mut reader = header.as_slice();
    let res = decode_message::<_, TestPrimaryRequest>(
        &mut reader,
        &mut decode_buffer,
        &mut compressed_buffer,
        max_chunk_size,
    )
    .await;

    // the decode fails because the body never arrives
    assert!(res.is_err(), "decode must fail when the body is withheld");

    // crucially, neither buffer was sized to the attacker-declared prefixes: with no body
    // delivered, both hold zero bytes rather than the ~2.15 MiB the prefixes declared
    assert_eq!(compressed_buffer.len(), 0, "no body arrived, so no compressed bytes are committed");
    assert_eq!(decode_buffer.len(), 0, "decompression never ran, so no bytes are committed");
}

/// Regression test for issue #728: a peer that declares a large compressed body but delivers only
/// part of it before closing commits memory proportional to what it actually sent, not the declared
/// prefix.
#[tokio::test]
async fn test_partial_body_commits_only_delivered_bytes() {
    let max_chunk_size = 1024 * 1024; // 1mb
    let compressed_len: u32 = 1_000_000;
    let delivered = 4096; // only a small slice of the declared body is sent

    let mut wire = malicious_header(1024 * 1024, compressed_len);
    wire.resize(wire.len() + delivered, 0);

    let mut decode_buffer = Vec::new();
    let mut compressed_buffer = Vec::new();
    let mut reader = wire.as_slice();
    let res = decode_message::<_, TestPrimaryRequest>(
        &mut reader,
        &mut decode_buffer,
        &mut compressed_buffer,
        max_chunk_size,
    )
    .await;

    // fails because fewer bytes arrived than the declared compressed length
    assert!(res.is_err(), "decode must fail on a short body");

    // only the delivered bytes were committed, far below the ~1 MB the prefix declared
    assert_eq!(
        compressed_buffer.len(),
        delivered,
        "committed memory tracks delivered bytes, not the declared compressed_len"
    );
}

/// Reader that yields a fixed header then stalls forever on the body read, modeling a peer that
/// sends the length prefixes and withholds the body.
struct HeaderThenStall {
    header: Vec<u8>,
    pos: usize,
}

impl futures::AsyncRead for HeaderThenStall {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if this.pos < this.header.len() {
            let n = (buf.len()).min(this.header.len() - this.pos);
            buf[..n].copy_from_slice(&this.header[this.pos..this.pos + n]);
            this.pos += n;
            std::task::Poll::Ready(Ok(n))
        } else {
            // body never arrives
            std::task::Poll::Pending
        }
    }
}

/// Regression test for issue #728: while parked awaiting a withheld body, the decoder holds only
/// the bytes that have arrived (none), not the declared prefixes.
#[tokio::test]
async fn test_withheld_body_holds_no_declared_memory_while_parked() {
    let mut reader = HeaderThenStall { header: malicious_header(1024 * 1024, 1_200_000), pos: 0 };
    let mut decode_buffer = Vec::new();
    let mut compressed_buffer = Vec::new();

    let fut = decode_message::<_, TestPrimaryRequest>(
        &mut reader,
        &mut decode_buffer,
        &mut compressed_buffer,
        1024 * 1024,
    );

    // the body never arrives; the read stays parked until our short test timeout fires (on the wire
    // the libp2p request timeout, 10s by default, governs the hold)
    let res = tokio::time::timeout(std::time::Duration::from_millis(200), fut).await;
    assert!(res.is_err(), "future stays parked at the body read while the body is withheld");

    // while parked, nothing proportional to the declared prefixes was committed
    assert_eq!(compressed_buffer.len(), 0, "no body bytes arrived while parked");
    assert_eq!(decode_buffer.len(), 0, "decompression never ran while parked");
}
