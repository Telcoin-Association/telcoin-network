//! Codec for encoding/decoding consensus network messages.

use async_trait::async_trait;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::{request_response::Codec, StreamProtocol};
use serde::{de::DeserializeOwned, Serialize};
use snap::read::FrameDecoder;
use std::{
    fmt,
    io::{Read as _, Write as _},
    marker::PhantomData,
};
use tn_types::encode;

/// Convenience type for all traits implemented for messages used for TN request-response codec.
pub trait TNMessage: Send + Serialize + DeserializeOwned + Clone + fmt::Debug + 'static {}

/// The Telcoin Network request/response codec for consensus messages between peers.
///
/// The codec reuses pre-allocated buffers to asynchronously read messages per the libp2p [Codec]
/// trait. All messages include a 4-byte prefix that indicates the message's uncompressed length.
/// Peers use this prefix to safely decompress and decode messages from peers.
///
/// TODO:
/// - handle peer scores when messages are malicious
/// - verify StreamProtocol between peers
#[derive(Clone, Debug)]
pub struct TNCodec<Req, Res> {
    /// The fixed-size buffer for compressed messages.
    compressed_buffer: Vec<u8>,
    /// The fixed-size buffer for requests.
    decode_buffer: Vec<u8>,
    /// The maximum size (bytes) for a single message.
    ///
    /// The 4-byte message prefix does not count towards this value.
    max_chunk_size: usize,
    /// Phantom data for codec that indicates network message type.
    _phantom: PhantomData<(Req, Res)>,
}

impl<Req, Res> TNCodec<Req, Res> {
    /// Create a new instance of Self.
    pub fn new(max_chunk_size: usize) -> Self {
        // create buffer from max possible compression length based on max request size
        let max_compress_len = snap::raw::max_compress_len(max_chunk_size);
        let compressed_buffer = Vec::with_capacity(max_compress_len);
        // allocate capacity for decoding max message size
        let decode_buffer = Vec::with_capacity(max_chunk_size);

        Self {
            compressed_buffer,
            decode_buffer,
            max_chunk_size,
            _phantom: PhantomData::<(Req, Res)>,
        }
    }

    /// Convenience method to keep READ logic DRY.
    ///
    /// This method is used to read requests and responses from peers.
    #[inline]
    async fn decode_message<T, M>(&mut self, io: &mut T) -> std::io::Result<M>
    where
        T: AsyncRead + Unpin + Send,
        M: TNMessage,
    {
        // clear buffers
        self.compressed_buffer.clear();
        self.decode_buffer.clear();

        // retrieve prefix for uncompressed message length
        let mut prefix = [0; 4];
        io.read_exact(&mut prefix).await?;

        // NOTE: cast usize to u32 is safe
        let length = u32::from_le_bytes(prefix) as usize;

        // ensure message length within bounds
        if length > self.max_chunk_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "prefix indicates message size is too large",
            ));
        }

        // resize buffer to reported message size
        //
        // NOTE: this should not reallocate
        self.decode_buffer.resize(length, 0);

        // take max possible compression size based on reported length
        // this is used to limit the amount read in case peer used malicious prefix
        //
        // NOTE: usize -> u64 won't lose precision (even on 32bit system)
        let max_compress_len = snap::raw::max_compress_len(length);
        io.take(max_compress_len as u64).read_to_end(&mut self.compressed_buffer).await?;

        // decompress bytes
        let reader = std::io::Cursor::new(&mut self.compressed_buffer);
        let mut snappy_decoder = FrameDecoder::new(reader);
        snappy_decoder.read_exact(&mut self.decode_buffer)?;

        // decode bytes
        bcs::from_bytes(&self.decode_buffer)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    /// Convenience method to keep WRITE logic DRY.
    ///
    /// This method is used to write requests and responses from peers.
    #[inline]
    async fn encode_message<T, M>(&mut self, io: &mut T, msg: M) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
        M: TNMessage,
    {
        // global encode
        let bytes = encode(&msg);
        println!("bytes? {:?}", bytes);

        // ensure encoded bytes are within bounds
        if bytes.len() > self.max_chunk_size {
            println!("bytes length greater than max allowable size: {:?}", self.max_chunk_size);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "encode data > max_chunk_size",
            ));
        }

        // length prefix for uncompressed bytes
        //
        // NOTE: 32bit max 4,294,967,295
        let prefix = (bytes.len() as u32).to_le_bytes();
        println!("prefix: {:?}", prefix);
        io.write_all(&prefix).await?;

        // compress data
        let mut encoder = snap::write::FrameEncoder::new(Vec::new());
        println!("writing now...");
        encoder.write_all(&bytes)?;
        println!("time to flush XD");
        encoder.flush()?;

        println!("flushed... writing all:\n{:?}\n\n", encoder.get_ref());

        // add compressed bytes to prefix
        io.write_all(encoder.get_ref()).await?;

        println!("returning Ok()...");
        Ok(())
    }
}

impl<Req, Res> Default for TNCodec<Req, Res> {
    fn default() -> Self {
        Self::new(MAX_REQUEST_SIZE)
    }
}

/// Max request size in bytes
///
/// Worker blocks are capped at 1MB worth of transactions.
/// This should be more than enough for snappy-compressed messages.
///
/// TODO: add the message overhead as the max request size
const MAX_REQUEST_SIZE: usize = 1024 * 1024;

#[async_trait]
impl<Req, Res> Codec for TNCodec<Req, Res>
where
    Req: TNMessage,
    Res: TNMessage,
{
    type Protocol = StreamProtocol;
    type Request = Req;
    type Response = Res;

    async fn read_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        self.decode_message(io).await
    }

    async fn read_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> std::io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        self.decode_message(io).await
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        self.encode_message(io, req).await
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> std::io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        self.encode_message(io, res).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use tn_test_utils::fixture_batch_with_transactions;
    use tn_types::{BlockHash, WorkerBlock};

    // impl TNMessage for compiler
    impl TNMessage for WorkerBlock {}

    #[derive(Serialize, Deserialize, Default, PartialEq, Clone, Debug)]
    struct TestData {
        timestamp: u64,
        base_fee_per_gas: Option<u64>,
        hash: BlockHash,
    }

    #[tokio::test]
    async fn test_encode_decode_same_message() {
        let max_chunk_size = 1024 * 1024; // 1mb
        let mut codec = TNCodec::<WorkerBlock, WorkerBlock>::new(max_chunk_size);
        let protocol = StreamProtocol::new("/test");

        // encode request
        let mut encoded = Vec::new();
        let block = WorkerBlock::default();
        codec
            .write_request(&protocol, &mut encoded, block.clone())
            .await
            .expect("write valid request");

        // now decode request
        let decoded =
            codec.read_request(&protocol, &mut encoded.as_ref()).await.expect("read valid request");
        assert_eq!(decoded, block);

        // encode response
        let mut encoded = Vec::new();
        let block = fixture_batch_with_transactions(100);
        codec
            .write_response(&protocol, &mut encoded, block.clone())
            .await
            .expect("write valid response");

        // now decode response
        let decoded = codec
            .read_response(&protocol, &mut encoded.as_ref())
            .await
            .expect("read valid response");
        assert_eq!(decoded, block);
    }

    #[tokio::test]
    async fn test_fail_to_write_message_too_big() {
        let max_chunk_size = 100; // 100 bytes is too small
        let mut codec = TNCodec::<WorkerBlock, WorkerBlock>::new(max_chunk_size);
        let protocol = StreamProtocol::new("/test");

        // encode request
        let mut encoded = Vec::new();
        let block = fixture_batch_with_transactions(1);
        let res = codec.write_request(&protocol, &mut encoded, block.clone()).await;
        assert!(res.is_err());

        // encode response
        let mut encoded = Vec::new();
        let block = fixture_batch_with_transactions(1);
        let res = codec.write_response(&protocol, &mut encoded, block.clone()).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_reject_message_prefix_too_big() {
        let max_chunk_size = 100; // 100 bytes is too small
        let mut honest_peer = TNCodec::<WorkerBlock, WorkerBlock>::new(max_chunk_size);
        let protocol = StreamProtocol::new("/test");
        // malicious peer writes legit messages that are too big
        // "legit" means correct prefix and valid data. the only problem is message too big
        let mut malicious_peer = TNCodec::<WorkerBlock, WorkerBlock>::new(1024 * 1024);

        //
        // test requests first
        //
        // sanity check that block within bounds works
        let mut encoded = Vec::new();
        let block = WorkerBlock::default(); // 72 bytes uncompressed
        malicious_peer
            .write_request(&protocol, &mut encoded, block.clone())
            .await
            .expect("write legit and valid request");
        let decoded = honest_peer
            .read_request(&protocol, &mut encoded.as_ref())
            .await
            .expect("read valid request");
        assert_eq!(decoded, block);

        // now encode legit message that's too big for honest peer
        let mut encoded = Vec::new();
        let block = fixture_batch_with_transactions(1);
        malicious_peer
            .write_request(&protocol, &mut encoded, block.clone())
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
        let block = WorkerBlock::default(); // 72 bytes uncompressed
        malicious_peer
            .write_response(&protocol, &mut encoded, block.clone())
            .await
            .expect("write legit and valid response");
        let decoded = honest_peer
            .read_response(&protocol, &mut encoded.as_ref())
            .await
            .expect("read valid response");
        assert_eq!(decoded, block);

        // now encode legit message that's too big for honest peer
        let mut encoded = Vec::new();
        let block = fixture_batch_with_transactions(1);
        malicious_peer
            .write_response(&protocol, &mut encoded, block.clone())
            .await
            .expect("write legit response");
        // prefix length should cause error
        let res = honest_peer.read_response(&protocol, &mut encoded.as_ref()).await;
        assert!(res.is_err())
    }

    #[tokio::test]
    async fn test_malicious_prefix_deceives_peer_to_read_message_and_fails() {
        let max_chunk_size = 100; // 100 bytes max message size
        let mut honest_peer = TNCodec::<WorkerBlock, WorkerBlock>::new(max_chunk_size);
        let protocol = StreamProtocol::new("/test");
        // malicious peer writes legit messages that are too big
        // "legit" means correct prefix and valid data. the only problem is message too big
        let mut malicious_peer = TNCodec::<WorkerBlock, WorkerBlock>::new(1024 * 1024);

        //
        // test requests first
        //
        // encode valid message that's too big and change prefix to deceive peer into trying to read
        // content
        let mut encoded = Vec::new();
        let block = fixture_batch_with_transactions(1);
        malicious_peer
            .write_request(&protocol, &mut encoded, block.clone())
            .await
            .expect("write legit request");
        // assert prefix is greater than peer's max chunk size
        let mut actual_prefix = [0; 4];
        actual_prefix.clone_from_slice(&encoded[0..4]);
        let actual_length = u32::from_le_bytes(actual_prefix) as usize;

        // sanity check
        assert!(actual_length > max_chunk_size);
        assert!(encoded.len() > max_chunk_size);

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
        let block = fixture_batch_with_transactions(1);
        malicious_peer
            .write_response(&protocol, &mut encoded, block.clone())
            .await
            .expect("write legit response");
        // assert prefix is greater than peer's max chunk size
        let mut actual_prefix = [0; 4];
        actual_prefix.clone_from_slice(&encoded[0..4]);
        let actual_length = u32::from_le_bytes(actual_prefix) as usize;

        // sanity check
        assert!(actual_length > max_chunk_size);
        assert!(encoded.len() > max_chunk_size);

        // manipulate prefix to obfuscate actual message size is too big
        // this sets prefix to the honest peer's max message length,
        // which is considered valid and within message size bounds
        encoded[0..4].clone_from_slice(&100u32.to_le_bytes());

        // should cause an unexpected EOF
        let res = honest_peer.read_response(&protocol, &mut encoded.as_ref()).await;
        assert!(res.is_err());
    }
}
