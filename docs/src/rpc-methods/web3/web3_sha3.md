# web3\_sha3

#### Parameters

`DATA` - The data to hash.

#### Returns

`DATA`, 32 Bytes - The Keccak-256 hash of the data. This is the hash the EVM's `KECCAK256` opcode computes, not the standardized SHA3-256.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"web3_sha3","params":["0x68656c6c6f20776f726c64"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": "0x47173285a8d7341e5e972fc677286384f802f8ef42a5ec5f03bbfa254cb01fad" // keccak256("hello world")
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#web3_sha3)
