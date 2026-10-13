# tn\_latestConsensusHeader

Returns the newest consensus header this node has seen. Consensus headers form the consensus chain: each one records a sub-DAG that BFT consensus committed, in order, and execution blocks are built from them. A validator in the committee reports the header its own consensus just committed; a node that follows consensus reports the newest header it has received and verified, which can be ahead of the blocks it has executed while it catches up. Until the node has seen a header after starting, the result is a placeholder header with `number` 0.

The result is served from memory and does not read consensus storage. [tn\_latestHeader](tn_latestheader.md) is a deprecated alias that returns the same object. Header digests, authority ids and signatures in this object are base58 strings; execution block hashes, batch digests, `randomness` and `extra` are `0x` hex.

#### Parameters

`None`

#### Returns

`Object` - The consensus header object. Its field names are snake\_case:

* `parent_hash`: `String` - Digest of the previous consensus header, 32 bytes, base58-encoded.
* `sub_dag`: `Object` - The committed sub-DAG:
  * `headers`: `Array` - The headers this commit orders, in commit order. The last one is the leader's. Each header object has:
    * `author`: `String` - Authority id of the validator that created the header, base58-encoded (see `authority_id` in [tn\_info](tn_info.md)).
    * `round`: `Number` - The consensus round of the header.
    * `epoch`: `Number` - The epoch the header was created in.
    * `created_at`: `Number` - When the header was created, in whole seconds since the Unix epoch.
    * `payload`: `Array` - The transaction batches the header includes, each a two-element array: the batch digest (`DATA`, 32 Bytes) and the id of the worker that produced it (`Number`).
    * `parents`: `Array` - Digests of the header's parent certificates from the previous round, base58-encoded.
    * `latest_execution_block`: `Object` - The latest execution block the author had when it built the header: `number` (`Number`) and `hash` (`DATA`, 32 Bytes).
    * `seed_signature`: `String` - The author's BLS signature over the epoch seed message for this round, base58-encoded; it feeds the epoch seed chain that shuffles the next committee. Present for epochs where the seed-signature fork is active: every epoch on mainnet builds, epoch 383 onward on adiri.
    * `created_at_millis`: `Number` - The millisecond part of `created_at`, `0` to `999`. Present only for epochs where the sub-second timestamp fork is active: every epoch on mainnet builds, and on adiri from a fork epoch that is not yet scheduled.
  * `reputation_scores`: `Object` - Leader reputation scores: `scores_per_authority` maps each authority id (base58) to its score (`Number`), and `final_of_schedule` (`Boolean`) is `true` when these are the last scores of the current leader schedule before they reset.
  * `commit_timestamp`: `Number` - The commit time, in whole seconds since the Unix epoch.
  * `randomness`: `DATA`, 32 Bytes - The epoch seed chain value as of this commit. For epochs before the seed-signature fork, a hash of the leader certificate's aggregate signature instead.
  * `commit_timestamp_millis`: `Number` - The millisecond part of `commit_timestamp`, `0` to `999`. Present only when the sub-second timestamp fork is active for the leader's epoch, as for `created_at_millis`. [tn\_getBlockTimestampMillis](tn_getblocktimestampmillis.md) reports the combined commit time for an execution block.
* `number`: `Number` - The header's height in the consensus chain.
* `extra`: `DATA`, 32 Bytes - Reserved; currently always zero.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"tn_latestConsensusHeader","params":[],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "parent_hash": "F7V4oK3SzU4Sh3TgjKtacJDqVvKG4xeBJoci5pWDM8cr",
    "sub_dag": {
      "headers": [
        {
          "author": "AFRwTgoeRcEDVUu6QLXkMs8qPouYtEdLmWu2HR6YRBuJ",
          "round": 8718,
          "epoch": 601,
          "created_at": 1791348194,
          "payload": [],
          "parents": [
            "6ubuD1mhZQnNioC6uZHHcdXT4Hqn9W42o1qQbTWeGy6b",
            "CDq3gKsxwGffYso6XZ9nC4XGC2WpmmGecs9owzGZ7i2h",
            "CiJR3e7SBw59gvCrsd71eTCHthiRa4dX2tjyh3Zhfwzn",
            "Dpj3DBvzEm65sj5wWTKSuJvj6bBdjU2eT2uQFPLSvEff"
          ],
          "latest_execution_block": {
            "number": 500829,
            "hash": "0x73bebb21d61d379e5a38510fe5cbf093a32c409778b04dff54832e97039ed894"
          },
          "seed_signature": "5qed98kzkQFKe2RrPm9Rt3qHtQe8D3dCQnFRj7FS38wCVEMExhRqYEccyUasBNCdyj"
        },
        // ... seven more headers ...
        {
          "author": "G6A8BRn31vofiVH8KZzETW2kcPsbomNTQYMgvZg52jTg", // the leader
          "round": 8720,
          "epoch": 601,
          "created_at": 1791348196,
          "payload": [],
          "parents": [
            "33Rkr46KvuYguKGpyztWdVFTJgKMQ7HMZJqSxq5VvBVB",
            "4FCmMaftVgW1LjFHSg4vaaQnQUeLnyp3DZ6mMSDwnxLd",
            "DRuiwXGYkP3tbqAkNm9ZW2X62GLoVtaGBuMRpAQVGoPm",
            "FF5fxaN4dYHQASC3hFpCLVVgf9qLsWbe4z1x8aJv6BZq"
          ],
          "latest_execution_block": {
            "number": 500829,
            "hash": "0x73bebb21d61d379e5a38510fe5cbf093a32c409778b04dff54832e97039ed894"
          },
          "seed_signature": "7PCVZwBmeVzPNdA9JP2oBhcHD58cKpQR7SEvXeuJYVZXWT7oGE7ALYFZS5GbARcTXT"
        }
      ],
      "reputation_scores": {
        "scores_per_authority": {
          "4LSGzE7N43UMvJoyTPTzvjjc9my2XvT6qjMDo8axRLgM": 49,
          "AFRwTgoeRcEDVUu6QLXkMs8qPouYtEdLmWu2HR6YRBuJ": 30,
          "AhVqH6fD3qLKDhrA3zSDQs9jaLR1h5uQ9QzYc6bCpfaH": 49,
          "FfsRxvnfYtW7umk9AGfH24BgNYnbkJ6tn2paTXJse99d": 29,
          "G6A8BRn31vofiVH8KZzETW2kcPsbomNTQYMgvZg52jTg": 39
        },
        "final_of_schedule": false
      },
      "commit_timestamp": 1791348196,
      "randomness": "0x1c10702d249a3a8b5ae183c4f2c790dca5de311e900dd72fe6b4e695d5148358"
    },
    "number": 7061244,
    "extra": "0x0000000000000000000000000000000000000000000000000000000000000000"
  }
}
```
