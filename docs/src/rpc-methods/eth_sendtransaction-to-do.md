---
description: >-
  Creates new message call transaction or a contract creation, if the data field
  contains code, and signs it using the account specified in from.
---

# eth\_sendTransaction (to do)

{% hint style="warning" %}
**Only for node operators**

The `eth_sendTransaction` JSON-RPC method requires a signature from the specified account. As nodes do not store user private keys, they cannot sign user transactions.&#x20;

Nodes can sign their own transactions but must unlock their account before doing so.
{% endhint %}

#### Parameters

`Object` - The transaction object

* `from`: `DATA`, 20 Bytes - The address the transaction is sent from.
* `to`: `DATA`, 20 Bytes - (optional when creating new contract) The address the transaction is directed to.
* `gas`: `QUANTITY` - (optional, default: 90000) Integer of the gas provided for the transaction execution. It will return unused gas.
* `gasPrice`: `QUANTITY` - (optional, default: To-Be-Determined) Hexadecimal of the gasPrice used for each paid gas.
* `value`: `QUANTITY` - (optional) Hexadecimal of the value sent with this transaction.
* `input`: `DATA` - The compiled code of a contract OR the hash of the invoked method signature and encoded parameters.
* `nonce`: `QUANTITY` - (optional) Hexadecimal nonce value. This allows to overwrite your own pending transactions that use the same nonce.

#### Returns

`DATA`, 32 Bytes - The transaction hash, or the zero hash if the transaction is not yet available.

Use [eth\_getTransactionReceipt](eth\_gettransactionreceipt.md) to get the contract address, after the transaction was mined, when you created a contract.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_sendTransaction","params":[  {
    from: "0xb60e8dd61c5d32be8058bb8eb970870f07233155",
    to: "0xd46e8dd67c5d32be8058bb8eb970870f07244567",
    gas: "0x76c0", // 30400
    gasPrice: "0x9184e72a000", // 10000000000000
    value: "0x9184e72a", // 2441406250
    input:
      "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675",
  }],"id":1}'
```

#### Result

```
{
  "id":1,
  "jsonrpc": "2.0",
  "result": "0xe670ec64341771606e55d6b4ca35a1a6b75ee3d5145a99d05921026d1527331"
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_sendtransaction)
