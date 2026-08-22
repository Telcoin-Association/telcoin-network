---
description: >-
  Returns the account and storage values, including the Merkle proof, of the
  specified account.
---

# eth\_getProof

#### Parameters

`address`: `DATA`, 20 Bytes - address to check for balance.

`storageKeys`: `Array`of `DATA` -  array of 32 bytes storage keys to be proofed and included.

`blockParameter`: `QUANTITY|TAG` - hexadecimal block number, or one of the string tags `latest`, `earliest`, `safe`, or `finalized`. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block).

#### Returns

* `balance`: `QUANTITY`-Hexadecimal of the current balance in wei.
* `codeHash`: `DATA`, 32 Bytes - Hash of the code of the account.
* `nonce`: `QUANTITY` - The nonce of the account.
* `storageHash`: `DATA`, 32 bytes - The SHA3 of the StorageRoot. All storage will deliver a Merkle proof starting with this `rootHash`.
* `accountProof`: `Array`of`DATA` - Array of RLP-serialized MerkleTree-Nodes, starting with the stateRoot-Node, following the path of the SHA3 (address) as key.
* `storageProof`: `Array`of`DATA` - Array of storage-entries as requested. Each entry is an object with these properties:
  * `key`: `DATA`- The requested storage key.
  * `value`: `QUANTITY`- The storage value.
  * `proof`: `Array`of `DATA` - Array of RLP-serialized MerkleTree-Nodes, starting with the storageHash-Node, following the path of the SHA3 (key) as path.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_getProof","params":[
        "0x4392743B97C46c6Aa186A7f3D0468fbF177ee70F",
        ["0x86386409a65c1a7f963bc51852fa7ecbdb9cad2cec464de22ee4591e1622b46b"],
        "latest"]
        ,"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "error": {
    "code": -32603,
    "message": "internal blocking task error"
  },
  "id": 1
}
```

[source](https://docs.infura.io/api/networks/ethereum/json-rpc-methods/eth\_getproof)
