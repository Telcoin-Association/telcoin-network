# Telcoin Network Node

This is the main crate for managing an active node on Telcoin Network.

## Node Types

Active validator nodes (in the current committee) are either in `CvvActive` or `CvvInactive` mode.
If the validator is Inactive, it indicates that it crashed during the epoch and is syncing to rejoin consensus within the garbage collection window.
A committee member starts its batch builders only while it is `CvvActive`, because a batch sealed by quorum must be reported to a running proposer.
While the member is not active, transactions it accepts stay in its local pool until a later epoch entry starts a builder: either it rejoins as `CvvActive`, or it enters an epoch outside the committee and forwards them to committee validators.
A member vetoed to `Observer` by a newer epoch record cannot rejoin within the epoch, so it holds them until the next epoch.

Observer nodes subscribe to the committee's consensus output and execute the data independently.

## Epoch Manager

The epoch manager is responsible for identifying the epoch boundary and advancing the next epoch.
It manages subtasks and sets the node's mode.
