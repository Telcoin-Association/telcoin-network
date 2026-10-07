# eth\_sendRawTransaction

Submits a pre-signed transaction to the network. This is the only way to send a transaction through a Telcoin Network node: nodes hold no user keys, so transactions must be signed client-side and submitted as raw bytes.

A node that is not part of the current committee accepts the transaction into its local pool and then forwards it to a committee validator's advertised RPC endpoint on your behalf. Forwarding is best-effort and routes by sender address, so all of one account's transactions converge on the same validator and nonce ordering is preserved. Submitting to any Telcoin Network node therefore works the same way.

Telcoin Network nodes additionally guard this method with a transaction fee cap (`--rpc.txfeecap`, in TEL; `0` disables the cap and is the default). When an operator sets a cap, the transaction's maximum possible fee - `maxFeePerGas * gasLimit`, plus the blob fee bound for EIP-4844 transactions - must not exceed it. An over-cap transaction is rejected before it reaches the pool with a JSON-RPC error (code `-32000`) of the form:

```
tx fee (7000000 wei) exceeds the configured cap (200000 wei)
```

#### Parameters

`DATA`, The signed transaction data.

#### Returns

`DATA`, 32 Bytes - The transaction hash.

Use [eth\_getTransactionReceipt](eth_gettransactionreceipt.md) to get the contract address, after the transaction was mined, when you created a contract.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xd46e8dd67c5d32be8d46e8dd67c5d32be8058bb8eb970870f072445675058bb8eb970870f072445675"],"id":1}'
```

#### Result

```
{
  "id":1,
  "jsonrpc": "2.0",
  "result": "0xe670ec64341771606e55d6b4ca35a1a6b75ee3d5145a99d05921026d15273311"
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_sendrawtransaction)
