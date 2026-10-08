# debug\_executionWitness

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

> [!WARNING]
> **Witness of an epoch-closing block**
>
> On a block that closes an epoch, the witness comes from a re-execution that runs the epoch-closing `applyIncentives` call with the node's current rewards counter rather than the counts the block distributed, so the witness for such a block can differ from the block's real execution. Other blocks are unaffected. Tracked in [#1589](https://github.com/Telcoin-Association/telcoin-network/issues/1589).

Re-executes a block from its parent's state, system calls included, and returns what a stateless client needs to run the block again: the trie nodes, contract code, account and slot keys, and headers the execution and the state-root computation read. [debug\_executionWitnessByBlockHash](debug_executionwitnessbyblockhash.md) does the same for a block hash.

#### Parameters

`block parameter`: `QUANTITY|TAG` \[_Required_] - Hexadecimal block number, or one of the string tags `latest`, `earliest`, `safe`, or `finalized`. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block). A block the node does not have fails with error `-32001` and a message such as `block not found: 0x5`.

#### Returns

`Object` - The execution witness:

* `state`: `Array` - `DATA` elements: the RLP-encoded account-trie and storage-trie nodes the execution and the state-root computation read.
* `codes`: `Array` - `DATA` elements: the bytecode of every contract the execution loaded or created.
* `keys`: `Array` - `DATA` elements: the unhashed keys behind the trie paths, 20-byte addresses for accounts and 32-byte slots for storage.
* `headers`: `Array` - `DATA` elements: RLP-encoded headers from the oldest block the execution read with `BLOCKHASH` up to the parent block. When the block never runs `BLOCKHASH`, this is the parent header alone.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_executionWitness","params":["0x1"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "state": [
      "0x..." // RLP-encoded trie nodes, abbreviated
    ],
    "codes": [
      "0x..." // contract bytecode, abbreviated
    ],
    "keys": [
      "0xb14d3c4f5fbfbcfb98af2d330000d49c95b93aa7",
      "0x0000000000000000000000000000000000000000"
      // and the other accounts and storage slots the block touched
    ],
    "headers": [
      "0xf9025ba00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a097d6d56ffab6b8f3167576431dcf70eabc1f0605385d7249e581911908b89fb9a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b901000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000080808401c9c38080846ac5cdc480a0000000000000000000000000000000000000000000000000000000000000000088000000000000000007a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4218080a00000000000000000000000000000000000000000000000000000000000000000a0e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // header of block 0x0, the parent
    ]
  }
}
```

[source](https://reth.rs/jsonrpc/debug)
