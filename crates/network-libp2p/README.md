# TN Network - libp2p

Telcoin Network requires peer-to-peer network communication to achieve consensus.

Consensus p2p networking was initially built using [anemo](https://github.com/mystenlabs/anemo.git).
However, the protocol is moving to libp2p for all network communication.

This crate is separate while there are two existing network solutions in the codebase.
Eventually, this crate will replace tn-network and be the only network interface for p2p communication.
