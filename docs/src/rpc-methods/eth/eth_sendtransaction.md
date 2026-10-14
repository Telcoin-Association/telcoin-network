# eth\_sendTransaction

> [!WARNING]
> **Not usable on Telcoin Network**
>
> The `eth_sendTransaction` JSON-RPC method asks the node to sign the transaction with the `from` account's key. Telcoin Network nodes do not store user private keys and configure no RPC signing accounts, so every call to this method fails with the `unknown account` error shown below.
>
> Sign transactions client-side and submit them with [eth\_sendRawTransaction](eth_sendrawtransaction.md) instead.

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

On Telcoin Network this method never returns a transaction hash. Because the node has no signer for any `from` address, every call returns a JSON-RPC error (code `-32602`) with the message `unknown account`.

Use [eth\_sendRawTransaction](eth_sendrawtransaction.md) to submit a transaction signed client-side, and [eth\_getTransactionReceipt](eth_gettransactionreceipt.md) to get the contract address, after the transaction was mined, when you created a contract.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_sendTransaction","params":[{
    "from": "0xb60e8dd61c5d32be8058bb8eb970870f07233155",
    "to": "0xd46e8dd67c5d32be8058bb8eb970870f07244567",
    "gas": "0x76c0",
    "gasPrice": "0x9184e72a000",
    "value": "0x9184e72a",
    "input": "0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"
  }],"id":1}'
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

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_sendtransaction)
