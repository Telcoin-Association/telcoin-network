---
description: >-
  Let's cover a slightly tougher RPC call which will introduce some more
  blockchain concepts.
---

# eth\_call

Here we demonstrate how to use `eth_call` using TypeScript scripts.&#x20;

### Method Parameters

From the [RPC method reference](../../../rpc-methods/eth\_call.md) we see `eth_call` takes a number of arguments, some of which are optional. These are recreated below:&#x20;

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

From this description, we can see we are expected to pass two variables:&#x20;

* an object, comprised of some or all of the keys within it (e.g. `from`, `to`), and their associated values.
* the `block parameter`

### Selecting Parameters

For this example, we will only use `eth_call` to read some data. As reading state from the blockchain does not involve changing state, we do not need gas. As a result, we will not include the `gas` , `maxPriorityFeePerGas`, `maxFeePerGas` or `gasPrice` parameters. Similarly, reading state from the blockchain does not require an account. Therefore, we are not required to provide a `from` argument to the method call. As we are not writing any values to the blockchain we can also omit the `value` parameter.

Thus, we only need to provide values for:

* &#x20;`to`, the address of the contract with which we wish to interact.
* `data`,  the information specifying the manner in which we interact with the contract.&#x20;
* `block parameter`, the block we would like to read from.

### Updating Request Script

We can begin by amending our code from the [previous example](eth\_blocknumber.md#building-the-script) to prepare for this new method call.

Previously, our script was:

```
import axios from "axios";

const rpcUrl = "https://rpc.adiri.tel";
const data = {
    jsonrpc: "2.0",
    method: "eth_blockNumber",
    params: [],
    id: 1,
  };

const headers = {
    "Content-Type": "application/json",
  };

async function fetchData() {
    const response = await axios.post(rpcUrl, data, { headers });
    console.log(response.data);
}

fetchData();
```

As we will still be using axios, calling the same URL and using the same protocol (JSON), most of our script remains the same as before. The only changes occur in the `data` variable, where we want to use the `eth_call` method instead of `eth_blockNumber`, and we will need to pass some information into `params`.&#x20;

Let's start by making these changes to the `data` variable. The new data variable will be of the form:

```
const data = {
      jsonrpc: "2.0",
      method: "eth_call",
      params: [
        {
          to: "",
          data: "",
        },
        "latest",
      ],
      id: 1,
    };
```

Here we see we have the `to`and `data`keys in our parameters object, and we will be querying at the `latest` block. We have also updated the method we are calling to `eth_call`. We now need to find values for `to`and `data`.

For this example, we will consider the Telcoin AUD token which has contract address `0x4392743B97C46c6Aa186A7f3D0468fbF177ee70F`. This will be our value for our `to` parameter.

### Function Selector

Telcoin stablecoins are modified ERC-20 tokens. For this example we will use the ERC-20 getter function 'name()' to demonstrate the use of eth\_call.&#x20;

Since we know 'name()' is a function on the smart contract, and we see it takes no arguments, we can pass this into the data field of our parameters object. There is one slight caveat - the EVM will not understand the function call 'name()', but will only accept the '0x' prefixed first four bytes (8 characters in hexadecimal) of the Keccak-256 hash of name(). These 5 bytes ('0x' + four bytes) are known as the _function selector_. Let's run through this:

The function selector for `name()` can be computed from its signature (`"name()"`), which hashes to:&#x20;

`06fdde0383f15d582d1a74511486c9ddf862a882fb7904b3d9fe9b8b8e58a796`

(This can be computed using an online Keccak-256 hash calculator such as [this one](https://emn178.github.io/online-tools/keccak\_256.html)). Taking the first 4 bytes (8 characters) gives:

`06fdde03`

and adding the '0x' prefix gives:

`0x06fdde03`

This is the string we place in the data field.&#x20;

### Updated Data Variable

Bringing this all together, our new data variable should be:

```
const data = {
      jsonrpc: "2.0",
      method: "eth_call",
      params: [
        {
          to: "0x4392743B97C46c6Aa186A7f3D0468fbF177ee70F",
          data: "0x06fdde03",
        },
        "latest",
      ],
      id: 1,
    };
```

### Updated Script

Our script should be:

```
import axios from "axios";

const rpcUrl = "https://rpc.adiri.tel";
const data = {
      jsonrpc: "2.0",
      method: "eth_call",
      params: [
        {
          to: "0x4392743B97C46c6Aa186A7f3D0468fbF177ee70F",
          data: "0x06fdde03",
        },
        "latest",
      ],
      id: 1,
    };

const headers = {
    "Content-Type": "application/json",
  };

async function fetchData() {
    const response = await axios.post(rpcUrl, data, { headers });
    console.log(response.data);
}

fetchData();
```

### Running the Script

To run the script, we first compile the TypeScript to JavaScript by running:

```
npx tsc
```

Then run this with the command:

```
node index.js
```

### Result

The console should give the following response:

```
{
  jsonrpc: '2.0',
  result: '0x0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000b54656c636f696e20415544000000000000000000000000000000000000000000',
  id: 1
}
```

Here we see the RPC node returns:

* The JSON-RPC protocol version, which matches the one we specified.
* The 'result' which in this case is the hexadecimal value representing the name of the token at the contract address `0x4392743B97C46c6Aa186A7f3D0468fbF177ee70F`.
* The id, which we had specified as 1 in our request.

### Decoding the Result

The result received from the RPC node is:

`0x0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000b54656c636f696e20415544000000000000000000000000000000000000000000`

This is likely not the expected result - we expected 'Telcoin AUD' as the output. We need to decode this string and see what we get. Starting from the most significant byte (the leftmost) we get:

#### '0x' prefix

The 0x prefix indicates the following string is in hexadecimal format. From this point on, the data is organised into 32 byte (64 character) chunks.

#### Offset

The first 32 bytes symbolise the offset of the value. In this case, the 32 bytes after the 0x prefix are: `0000000000000000000000000000000000000000000000000000000000000020`. This value, converted from hexadecimal, is 32 (it contains a '2' in the '16's column, signifying 32). This means that the actual data starts in 32 bytes time (i.e. at byte 64). The 32 bytes in between this offset value and the actual data gives us the length of the data.

#### Length

The next 32 bytes, `0000000000000000000000000000000000000000000000000000000000000b`, tell us the data we are interested in is 11 bytes long.

#### Actual data

Reading 11 bytes (22 characters) from byte 64 (the next byte along) gives: `54656c636f696e20415544`. This can be converted directly from hex to a string using an online tool such as [this one](https://string-functions.com/hex-string.aspx). It translates to 'Telcoin AUD', exactly as expected!&#x20;

The remaining zeroes in the response are just padding to ensure each section of data is 32 bytes long.

### Conclusion

And there we have it! We've learned to use scripts to query more advanced data straight from the blockchain with very few dependencies. We've also learned how to decode data we receive from RPC nodes.

