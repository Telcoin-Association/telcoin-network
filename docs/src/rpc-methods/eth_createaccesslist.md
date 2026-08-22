# eth\_createAccessList

Creates an [EIP-2930](https://eips.ethereum.org/EIPS/eip-2930) access list that you can include in a transaction.

#### Parameters

`from`: `DATA`, 20 bytes - (optional) The address the transaction is sent from.

`to`: `DATA`, 20 bytes - The address the transaction is directed to.

`gas`: `QUANTITY` - (optional) Hexadecimal value of the gas provided for the transaction execution.&#x20;

`gasPrice`: `QUANTITY` - (optional) Hexadecimal value of the `gasPrice` , in Wei, provided by the sender. The default is `0`. used only in non-EIP-1559 transactions.

`maxPriorityFeePerGas`: `QUANTITY` - (optional) Hexadecimal of the maximum fee, in Wei, the sender is willing to pay per gas above the base fee. If used, must specify `maxFeePerGas` .

`maxFeePerGas`: `QUANTITY` - (optional) Hexadecimal of the maximum total fee (base fee + priority fee), in Wei, the sender is willing to pay per gas. If used, must specify `maxPriorityFeePerGas` .&#x20;

`value`: `QUANTITY` - Hexadecimal of the value sent with this transaction.

`data`: `DATA` - (optional) Hash of the method signature and encoded parameters. See [Ethereum contract ABI specification](https://docs.soliditylang.org/en/latest/abi-spec.html).

`block parameter`: `QUANTITY|TAG` \[_Required_]  - Hexadecimal value of the block number, or one of the string tags `latest`, `earliest`, `safe`, or `finalized`. See the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block).

#### Returns

Access list object with the following fields:

`accessList` - A list of objects with the following fields:

* `address`: `DATA`, 20 bytes - Addresses to be accessed by the transaction.
* `storageKeys`: `DATA` - Storage keys to be accessed by the transaction.

`gasUsed`:  `QUANTITY` - Hexadecimal string representing the approximate gas cost for the transaction if the access list is included.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_createAccessList","params":[{
        "from": "0xaeA8F8f781326bfE6A7683C2BD48Dd6AA4d3Ba63", 
        "data": "0x608060806080608155"}, 
        "finalized"],"id":1}'
```

#### Result

```
{
  jsonrpc: '2.0',
  result: { accessList: [ {address: '0x45e5af19ae13aac19215266edfab04f8db6f2c53',storageKeys: ['0x0000000000000000000000000000000000000000000000000000000000000081']} ], gasUsed: '0x12e92' },
  id: 1
}
```

[source](https://docs.infura.io/api/networks/ethereum/json-rpc-methods/eth\_createaccesslist)
