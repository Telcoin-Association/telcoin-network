---
description: Returns the value from a storage position at a given address.
---

# eth\_getStorageAt

#### Parameters

`DATA`, 20 Bytes - Address of the storage.

`QUANTITY` - Hexadecimal of the position in the storage.

`QUANTITY|TAG` - Hexadecimal block number, or the string `"latest"`, `"earliest"`, `"safe"`, `"finalized"`, see the [default block parameter](https://ethereum.org/en/developers/docs/apis/json-rpc/#default-block)

#### Returns

`DATA` - The value at this storage position.

#### Example

In this case we consider the deployed eAUD token at address   `0x4392743B97C46c6Aa186A7f3D0468fbF177ee70F` . From the token's documentation, we see the `'DECIMALS_SLOT'` is at address `0x86386409a65c1a7f963bc51852fa7ecbdb9cad2cec464de22ee4591e1622b46b`. Getting the storage at this address should give us the decimal value of the token.&#x20;

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0", "method": "eth_getStorageAt", "params": [
        "0x4392743B97C46c6Aa186A7f3D0468fbF177ee70F", 
        "0x86386409a65c1a7f963bc51852fa7ecbdb9cad2cec464de22ee4591e1622b46b", 
        "latest"], "id": 1}'
```

#### Result

```
{
  "jsonrpc": "2.0",
  "result": "0x0000000000000000000000000000000000000000000000000000000000000006", //6
  "id": 1
}
```

[source](https://ethereum.org/en/developers/docs/apis/json-rpc/#eth\_getstorageat)
