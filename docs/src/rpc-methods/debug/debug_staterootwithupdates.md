# debug\_stateRootWithUpdates

> [!NOTE]
> Served only when `debug` is named in `--http.api` or `--ws.api`; see [Enabling Namespaces](../enabling-namespaces.md).

Applies a set of account and storage changes, keyed by hashed address and hashed slot, on top of a block's state and returns the resulting state root together with the trie updates that root computation produced. Nothing is written to the node's database.

#### Parameters

`Object` - The hashed post state. Both fields are required; pass `{}` for an empty map:

* `accounts`: `Object` - Keyed by the Keccak-256 hash of an address. Each value is `null` for a destroyed account, or an object with `nonce` (`Number`, a decimal JSON number), `balance` (`QUANTITY`) and `bytecode_hash` (`DATA`, 32 Bytes, or `null` for an account without code).
* `storages`: `Object` - Keyed by the Keccak-256 hash of an address. Each value is an object with `wiped` (`Boolean`, whether the account's existing storage is cleared first) and `storage` (an object keyed by the Keccak-256 hash of a slot, with `QUANTITY` values).

`block parameter`: `QUANTITY|TAG|DATA` - (optional) Hexadecimal block number, or one of the string tags `latest`, `earliest`, `safe`, or `finalized`; a 32-byte block hash is also accepted. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block). Defaults to `latest`.

#### Returns

`Array` - Two elements:

1. `DATA`, 32 Bytes - The state root after the changes. With an empty hashed state it is the block's own state root.
2. `Object` - The trie updates, with snake_case field names:
   * `account_nodes`: `Object` - Updated account-trie branch nodes, keyed by the node's packed nibble path as hex without `0x`. Each node has `state_mask`, `tree_mask` and `hash_mask` (numbers), `hashes` (an array of 32-byte hashes) and `root_hash` (a 32-byte hash or `null`).
   * `removed_nodes`: `Array` - Removed account-trie node paths, as hex strings without `0x`.
   * `storage_tries`: `Object` - Keyed by hashed address. Each value has `is_deleted` (`Boolean`), `storage_nodes` and `removed_nodes`, in the same forms as the account-trie fields.

#### Example

#### Request

```
curl http://localhost:8545 \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"debug_stateRootWithUpdates","params":[{"accounts":{},"storages":{}},"latest"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    "0x10620e8dae725248ad5102bde4780354184a4c99b67d31cc857879b93b56c12a", // state root of the latest block
    {
      "account_nodes": {},
      "removed_nodes": [],
      "storage_tries": {}
    }
  ]
}
```

[source](https://reth.rs/jsonrpc/debug)
