---
description: Returns the number of transactions sent from an address.
---

# eth\_getTransactionCount

#### Parameters

`DATA`, 20 Bytes - Address.

`QUANTITY|TAG` -Hexadecimal block number, or the string `"latest"`, `"earliest"`, `"safe"` or `"finalized"`, see the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block)

#### Returns

`QUANTITY` - Hexadecimal of the number of transactions send from this address.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_getTransactionCount","params":[
        "0x3da87b1c3743bd2da60df2ef1bc6f26ef9da6086", 
        "latest"],"id":1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "result": "0x2c3", //707
  "id": 1
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_gettransactioncount)
