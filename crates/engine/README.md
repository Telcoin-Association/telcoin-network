# Engine

Telcoin Network engine executes consensus output to EVM-style blocks.

## Overview

Payloads correspond to a batch from the subdag that reached consensus.
Each batch is a separate payload that is executed in a separate block environment within the EVM.

Workers do not produce empty batches, but primaries always produce a `Header` for each round of consensus.
If there are no batches in the `ConsensusOutput`, a single EVM block is produced to accumulate rewards for the leader.

Block rewards are only applied to the leader for each round.
