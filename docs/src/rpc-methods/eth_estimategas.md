# eth\_estimateGas

Generates and returns an estimate of how much gas is necessary to allow the transaction to complete. The transaction will not be added to the blockchain. Note that the estimate may be significantly more than the amount of gas actually used by the transaction, for a variety of reasons including EVM mechanics and node performance.

#### Parameters

`from`: `DATA`, 20 bytes - (optional) The address the transaction is sent from.

`to`: `DATA`, 20 bytes - (optional) The address the transaction is directed to.

`gas`: `QUANTITY` - (optional) Hexadecimal value of the gas provided for the transaction execution. `eth_call` consumes zero gas, but this parameter may be needed by some executions.

`gasPrice`: `QUANTITY` - (optional) Hexadecimal value of the `gasPrice` used for each paid gas.

`maxPriorityFeePerGas`: `QUANTITY` - (optional) Hexadecimal value of the `gasPrice` , in Wei, provided by the sender. The default is `0`. used only in non-EIP-1559 transactions.

`maxFeePerGas`: `QUANTITY` - (optional) Hexadecimal of the maximum total fee (base fee + priority fee), in Wei, the sender is willing to pay per gas. If used, must specify `maxPriorityFeePerGas` .&#x20;

`value`: `QUANTITY` - (optional) Hexadecimal of the value sent with this transaction.

`data`: `DATA` - (optional) Hash of the method signature and encoded parameters. See [Ethereum contract ABI specification](https://docs.soliditylang.org/en/latest/abi-spec.html).

`block parameter`: `QUANTITY|TAG` \[_Required_] - Hexadecimal block number, or one of the string tags `latest`, `earliest`, `pending`, `safe`, or `finalized`. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block).

If no gas limit is specified geth uses the block gas limit from the pending block as an upper bound. As a result the returned estimate might not be enough to executed the call/transaction when the amount of gas is higher than the pending block gas limit.

#### Returns

`QUANTITY` - Hexadecimal of the estimate of the amount of gas used for the given transaction

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_estimateGas","params":[{
        "from": "0x9cE564c7d09f88E7d8233Cdd3A4d7AC42aBFf3aC", 
        "to": "0xd46e8dd67c5d32be8058bb8eb970870f07244567", 
        "value": "0x0"}],"id":1}'
```

#### Result

```
{
  "id":1,
  "jsonrpc": "2.0",
  "result": "0x5208" // 21000
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_estimategas)
