# Telcoin Network Node

This is the main crate for managing an active node on Telcoin Network.

## Node Types

Active validator nodes (in the current committee) are either in `CvvActive` or `CvvInactive` mode.
If the validator is Inactive, it indicates that it crashed during the epoch and is syncing to rejoin consensus within the garbage collection window.

Observer nodes subscribe to the committee's consensus output and execute the data independently.

## Epoch Manager

The epoch manager is responsible for identifying the epoch boundary and advancing the next epoch.
It manages subtasks and sets the node's mode.
