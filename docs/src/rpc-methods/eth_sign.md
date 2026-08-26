# eth\_sign

> [!WARNING]
> **Not usable on Telcoin Network**
>
> The `eth_sign` JSON-RPC method asks the node to sign a message with the given account's key. Telcoin Network nodes do not store user private keys and configure no RPC signing accounts, so every call to this method fails with the `unknown account` error shown below.
>
> Sign messages client-side with your own wallet or key management instead.

#### Parameters

`DATA`, 20 Bytes - Address

`DATA`, N Bytes - Message to sign

#### Returns

On Telcoin Network this method never returns a signature. Because the node has no signer for any address, every call returns a JSON-RPC error (code `-32602`) with the message `unknown account`.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_sign","params":["0x9b2055d370f73ec7d8a03e965129118dc8f5bf83", "0xdeadbeaf"],"id":1}'
```

#### Result

```
{
  "id":1,
  "jsonrpc": "2.0",
  "error": {
    "code": -32602,
    "message": "unknown account"
  }
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_sign)
