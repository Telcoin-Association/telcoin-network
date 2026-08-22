---
description: >-
  Calculates an Ethereum specific signature with: sign(keccak256("\x19Ethereum
  Signed Message:\n" + len(message) + message))).
---

# eth\_sign (todo)

{% hint style="warning" %}
**Only for node operators**

The `eth_sign` JSON-RPC method requires a signature from the specified account. As nodes do not store user private keys, they cannot sign user transactions.&#x20;

Nodes can sign their own transactions but must unlock their account before doing so.
{% endhint %}

#### Parameters

`DATA`, 20 Bytes - Address

`DATA`, N Bytes - Message to sign

#### Returns

`DATA` - Signature

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
  "result": "0xa3f20717a250c2b0b729b7e5becbff67fdaef7e0699da4de7ca5895b02a170a12d887fd3b17bfdce3481f10bea41f45ba9f709d39ce8325427b57afcfc994cee1b"
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_sign)
