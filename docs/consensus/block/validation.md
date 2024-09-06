# Validation
The header is proposed by the primary for each round. The header includes limited information so the other nodes can "pull" the data they need. Once all digests in the primary's header's `payload` field are validated by it's workers, the primary votes in favor of the header.

The primary header includes a `SealedHeader` produced by the engine. The digest is calculated with this value to ensure validation for peers.

Primary headers are validated once every batch in the payload is recovered by it's digest, the hashes match, and the transactions within a batch are validated.
