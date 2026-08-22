---
description: >-
  So far we have looked at reading blockchain data directly from the blockchain
  by calling RPC nodes using HTTP requests. We've also seen how to decode
  responses. Much of this can be abstracted away.
---

# Libraries

## Ethers.js

[Ethers.js](https://docs.ethers.org/v6/) is a library which abstracts away many of the complexities associated with interacting with EVM blockchains.&#x20;

Here we will run through the eth\_call example in two different ways, this time using ethers.js.

First, we need to install ethers into our project.

#### Installation

Simply install ethers using node package manager (npm):

```
npm install ethers
```

#### Setup

Create a new script named 'ethers\_call.ts' and open it.

We begin by importing ethers into our script. It is easiest to import everything from ethers to begin with. This is done with the line:

```typescript
import { ethers } from "ethers";
```

From here, we need to connect to the RPC node (ethers still needs to know where to get the information we need from). This introduces the idea of a `provider`. A [Provider](https://docs.ethers.org/v6/api/providers/#Provider) is a read-only connection to the blockchain. Ethers also has another object, the [Sigher](https://docs.ethers.org/v6/api/providers/#Signer), which uses an RPC node along with a private key to allow write access to the blockchain. We will cover this later.

We create a provider instance with the line:

```typescript
const provider = new ethers.JsonRpcProvider("https://rpc.adiri.tel");
```

&#x20;This establishes the connection to the RPC node and gives us an object on which we can call ethers functions.

#### Simple use

A simple function to start is just getting the block number. If we write the script:

```typescript
import { ethers } from "ethers";

// Set up the provider to connect to an Ethereum node
const provider = new ethers.JsonRpcProvider("https://rpc.adiri.tel");

async function fetchData() {
  console.log(await provider.getBlockNumber());
}
fetchData();
```

and run it, we should print the current block number to the console. Note it prints a decimal integer value, rather than the hexadecimal we got when calling the node using HTTP requests. Ethers handles the conversion automatically.

### Using call()

Our eth\_call example becomes slightly simpler too:

```typescript
import { ethers } from "ethers";

// Set up the provider to connect to an Ethereum node
const provider = new ethers.JsonRpcProvider("https://rpc.adiri.tel");

async function fetchData() {
  const result = await provider.call({
    to: "0x4392743B97C46c6Aa186A7f3D0468fbF177ee70F",
    data: "0x06fdde03",
  });

  // Log the result
  console.log("Response Data:", result);
}

fetchData();
```

This, when run should give the response:

`0x0000000000000000000000000000000000000000000000000000000000000020000000000000000000000000000000000000000000000000000000000000000b54656c636f696e20415544000000000000000000000000000000000000000000` as before.

### Directly calling functions

There is, however, an alternative approach to getting the name of the token, or calling any other read function on a given contract. This involves creating an ethers `Contract` instance, which requires a contract address, a provider and the contract Application Binary Interface (ABI). We have the first two, and the third is available [here](../../reference/stablecoin-contracts.md). The ABI provides ethers with important context relating to a contract's functionality and how to interact with it.

#### Handling the ABI

In your project's root folder, create a file named 'ABI.json' and copy and paste the ABI from the [documentation link](../../reference/stablecoin-contracts.md). The ABI is produced by the solidity compiler and is derived from the solidity smart contract code.

We can then import this information to our script using the built in fs (File System) and path modules:

```typescript
const abiPath = path.join(__dirname, 'ABI.json');
const abiJson = fs.readFileSync(abiPath, 'utf8');
const abi = JSON.parse(abiJson);
```

In the above 3 lines we:

1. create the string representing the path to the ABI.json file
2. Read the data stored within the ABI.json file
3. convert the read data to JSON format for use with ethers.

#### Creating the Contract Object&#x20;

We now have all the components required to create an ethers `Contract` object which we can do in the following way:&#x20;

```typescript
// Create the contract instance with the provider
  const contract = new ethers.Contract(
    "0x4392743B97C46c6Aa186A7f3D0468fbF177ee70F",
    abi,
    provider
  );
```

&#x20;Here we have provided the contract address, ABI and provider to create a new instance of a `Contract` object which we save to the variable named `contract`.  More information can be found in the [relevant section](https://docs.ethers.org/v6/api/contract/#Contract) of the documentation.

All that is left is for us to use the contract. We can do this easily by calling the function names as they appear on the smart contract. In our case, we know the token inherits from the ERC 20 token standard. It will therefore have a getter function named name() (this is the one we have been calling up to now). To get the name of the token we write:

```typescript
const name = await contract.name();
```

#### Building the Script

Putting all of these elements together might give us something like:

```typescript
import { ethers } from "ethers";
import fs from "fs";
import path from "path";

const provider = new ethers.JsonRpcProvider("https://rpc.adiri.tel");

async function fetchData() {
  const abiPath = path.join(__dirname, "ABI.json"); // Adjust the path as necessary
  const abiJson = fs.readFileSync(abiPath, "utf8");
  const abi = JSON.parse(abiJson);

  const contract = new ethers.Contract(
    "0x4392743B97C46c6Aa186A7f3D0468fbF177ee70F",
    abi,
    provider
  );

  const name = await contract.name();
  console.log("Contract Name:", name);
}

fetchData();
```

Feel free to overwrite the previous code from the ['Using call()' section](libraries.md#using-call).

#### Running the Script

We run this with:

```
npx tsc
```

which compiles our typescript scripts to JavaScript, followed by:

```
node ethers_call.js
```

#### Response

The script should return:

```
Contract Name: Telcoin AUD
```

Note the string has been automatically converted from hexadecimal.&#x20;

#### Reading Decimals

It is also very easy to call other functions on the contract - we just change the function name. For example, we can now easily query how many decimals this contract uses, by replacing:

```typescript
const name = await contract.name();
```

with:

```typescript
const name = await contract.decimals();
```

recompiling and rerunning the script gives:

`Contract Name: 6n`

Showing the contract uses 6 decimals. This is correct.

### Conclusion

Hopefully this series of tutorials has demonstrated how to query a blockchain, and the advantages of using libraries to gather data rather than raw HTTP requests. This should serve as useful context as you continue to learn about Telcoin Network!
