---
description: >-
  Executes a new message call immediately without creating a transaction on the
  block chain. Often used for executing read-only smart contract functions, for
  example the balanceOf for an ERC-20 contract
---

# eth\_call

#### Parameters

`Object` - The transaction call object:

* `from`: `DATA`, 20 bytes - (optional) The address the transaction is sent from.
* `to`: `DATA`, 20 bytes - The address the transaction is directed to.
* `gas`: `QUANTITY` - (optional) Hexadecimal value of the gas provided for the transaction execution.&#x20;
* `eth_call` consumes zero gas, but this parameter may be needed by some executions.
* `gasPrice`: `QUANTITY` - (optional) Hexadecimal value of the `gasPrice` used for each paid gas.
* `maxPriorityFeePerGas`: `QUANTITY` - Hexadecimal maximum fee, in Wei, the sender is willing to pay per gas above the base fee.
* `maxFeePerGas`: `QUANTITY` - Hexadecimal maximum total fee (base fee + priority fee), in Wei, the sender is willing to pay per gas.
* `value`: `QUANTITY` - Hexadecimal of the value sent with this transaction.
* `data`: `DATA` - (optional) Hash of the method signature and encoded parameters. See [Ethereum contract ABI specification](https://docs.soliditylang.org/en/latest/abi-spec.html).

`block parameter`: `QUANTITY|TAG` \[_Required_] - Hexadecimal block number, or one of the string tags `latest`, `earliest`, `safe`, or `finalized`. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block).

#### Returns

`DATA` - The return value of the executed contract

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_call","params":[{
  "to":"0x4392743B97C46c6Aa186A7f3D0468fbF177ee70F",
  "gas":"0x76c0",
  "gasPrice":"0x0",
  "value":"0x0",
  "data":"0x06fdde03"},
  "latest"],"id":1}'
```

#### Result

```
{
"jsonrpc":"2.0",
"result":"0x0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000b54656c636f696e20415544000000000000000000000000000000000000000000", // Telcoin AUD
"id":1
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_call)
