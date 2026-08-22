---
description: Returns the balance of the account of given address.
---

# eth\_getBalance

#### Parameters

`DATA`, 20 Bytes - Address to check for balance.

`QUANTITY|TAG` - Hexadecimal integer block number, or the string `"latest"`, `"earliest"`, `"safe"`, or `"finalized"`, see the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block)

#### Returns

`QUANTITY` - Hexadecimal of the current account balance in wei.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x3da87b1c3743bd2da60df2ef1bc6f26ef9da6086", "latest"],"id":1}'
```

#### Result

```
{
"jsonrpc":"2.0",
"result":"0xfbea2ff545df4530c6", // 4.6470077e+21
"id":1
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_getbalance)
