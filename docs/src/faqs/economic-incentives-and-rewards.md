# Economic Incentives and Rewards

Telcoin Network’s economic model creates alignment between validator interests and network success through carefully structured incentives. Block rewards follow a dynamic issuance model based on consensus participation and leadership roles within committees. Transaction fees, particularly priority fees, are awarded to block proposers to incentivize maximum transaction processing. The validator’s stake requirement ensures long-term alignment with network success, while competitive operational expenses maintain attractive returns relative to capital commitment.

Each validator in the committee operates an independent transaction pool to propose unique batches for the next block. The committee is responsible for validating the proposed transactions before deterministically ordering all batches of transactions to produce the next consensus block. Consensus blocks have a leader for the committed round, and the leader receives the block rewards at the end of the committee’s term. The period of time a committee is responsible is called an “epoch”. Block rewards for all leaders are automatically withdrawn from a governance safe at the end of each epoch.

The consensus block is then executed, which results in the beneficiary’s account balance increasing. An important distinction from other protocols is that each batch within the consensus block is executed as a separate block in the EVM. This is how the protocol can support multiple basefees in parallel, something that ultimately allows MNOs to protocolize “Quality-of-Service” within the same blockchain.

The two greatest costs to committee validators are computation during EVM execution and uploading batches of transactions for consensus. The cost of computation during execution is recovered through basefees and priority tips. The batch producer receives all the priority fees from transactions that make it to the final executed block. 100% of each transaction’s basefee is transferred to the governance safe to fund future block rewards.

In addition to transaction fees, the leader of the consensus block receives the reward for the round. Telcoin Association governance specifies the amount of block rewards per leader per epoch by updating the consensus registry on-chain. The amount of TEL per block reward is adjustable so rewards can adjust based on overall transaction volume.

One current limitation of the protocol allows for the possibility of duplicate transactions within competing batches from different validators. The first executed batch that contains this transaction receives the priority fees if there are any. However, basefees and block rewards remain the same.

<br>
