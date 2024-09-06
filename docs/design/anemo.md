# Anemo Network
The `anemo::Network` is used for client/server p2p communications.

The `NetworkClient` is used right now for all Primary/Worker/Engine local communication.
The client implements traits defined within the `traits.rs` mod and is a convenience approach for using local channels.

I'm not sure it's the best approach, but it works for now and is consistent.

## Details
`lattice-network` crate has traits and a network client that implements them.

These "local" traits use the built traits during the `build.rs` in tn-types. (which needs to be moved into its own crate for building the proto).
