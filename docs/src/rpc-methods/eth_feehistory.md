---
description: Returns historical gas information, allowing you to track trends over time.
---

# eth\_feeHistory

#### Parameters

`QUANTITY` - Hexadecimal number of blocks in the requested range. Between 1 and 1024 blocks can be requested in a single query. If blocks in the specified block range are not available, then only the fee history for available blocks is returned.

`QUANTITY|TAG` - Hexadecimal representing the highest number block of the requested range, or one of the string tags `latest` or `earliest`.

`Array` of `QUANTITY` - (optional) A monotonically increasing list of integer percentile values to sample from each block's effective priority fees per gas in ascending order, weighted by gas used.

#### Returns

`oldestBlock`: `QUANTITY` - Hexadecimal of the lowest number block of the returned range.

`baseFeePerGas`: `Array of QUANTITY` - An array of hexadecimal block base fees per gas, including an extra block value. The extra value is the next block after the newest block in the returned range. Returns zeroes for blocks created before [EIP-1559](https://eips.ethereum.org/EIPS/eip-1559).

`gasUsedRatio`: `Array of QUANTITY` - An array of hexadecimal block gas used ratios. These are calculated as the ratio of `gasUsed` and `gasLimit`.

`reward`: `Array of QUANTITY` - An array of hexadecimal effective priority fee per gas data points from a single block. All zeroes are returned if the block is empty.

#### Example

#### Request

```
curl https://rpc.adiri.tel \
 -X POST \
 -H "Content-Type: application/json" \
 --data '{"jsonrpc":"2.0","method":"eth_feeHistory","params":[
         "0x5", 
         "latest", 
         [20, 30]],"id":1}'
```

#### Result

```
{
  "id":1,
  "jsonrpc": "2.0",
  "result": {
    baseFeePerGas: [ '0x7', '0x7', '0x7', '0x7', '0x7', '0x7' ],
    gasUsedRatio: [ 0, 0, 0, 0, 0 ],
    oldestBlock: '0x667017',
    reward: [ ["0x0","0x0"], ["0x0","0x0"],
      ["0x0","0x0"],["0x0","0x0"],
      ["0x0","0x0"] ]
  }
}
```

[source](https://docs.infura.io/api/networks/ethereum/json-rpc-methods/eth\_feehistory)
