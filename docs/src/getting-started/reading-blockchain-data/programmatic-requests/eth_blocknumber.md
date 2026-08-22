---
description: >-
  As a simple example, we can use TypeScript and axios (A typescript library for
  making API requests) to query the current block number.
---

# eth\_blockNumber

#### Building the Script

1. Create a TypeScript file in the project folder you created above:

```
touch index.ts
```

2. Open the file and paste the following code in:

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

3. Let's run through what each line does:

```
import axios from "axios";
```

This line imports the Axios library, which we installed earlier using npm. It is used for making HTTP requests (similar to curl).

```
const rpcUrl = "https://rpc.adiri.tel";
```

This creates a new variable named `rpcUrl` to which we assign the url of the Telcoin RPC server.

```
const data = {
    jsonrpc: "2.0",
    method: "eth_blockNumber",
    params: [],
    id: 1,
};
```

This creates an object named `data` where we include the data for our request. This layout is standard for JSON-RPC requests within Ethereum Virtual Machine (EVM) based networks (of which Telcoin Network is one). The individual fields are:

* `jsonrpc`: Specifies the JSON-RPC protocol version, which is `2.0` for EVM requests.&#x20;
* `method`: Indicates the JSON-RPC method being called, `eth_blockNumber` in this case. This method returns the number of the most recent block.
* `params`: An array of parameters to be passed to the method. As `eth_blockNumber` takes no input arguments, parameters is left as an empty list.
* `id`: A unique identifier for the request. This is used to match responses with requests when multiple JSON-RPC calls are made asynchronously. This can be set to any integer.

```
const headers = {
    "Content-Type": "application/json",
};
```

This is used to inform the server that the request body format is JSON. This is always the case with RPC calls.

```
async function fetchData() {
        const response = await axios.post(rpcUrl, data, { headers });
        console.log(response.data);
}
```

This is the main function to fetch the desired data. Note that it is labelled as `async`. This allows us to use the `await` keyword to handle promises asynchronously. Also note it takes no arguments.

The main thing this does is use `axios.post` to make a POST request to the RPC URL. It sends the `data` object we created earlier and includes the `headers`. This is all required because that's just how HTTP requests work!

Upon running the line `const response = await axios.post(rpcUrl, data, { headers })`, axios posts the content to the RPC node and immediately receives a [promise](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global\_Objects/Promise). This response will eventually resolve to the response of the HTTP request. The `await` keyword pauses execution of the script until the promise resolves and returns the desired information. This requested information, which in our case is the blocknumber, is saved to a variable named `response`.

`response` is another object containing a number of key-value pairs. Of these keys, we are interested in the `data` key, so we access this directly using `response.data`. This is then printed to the console (or terminal in our case).

```
fetchData();
```

Finally, fetch data is called. This runs the code within the `fetchData` function.

#### Running the Script

To run the script, we first compile the TypeScript to JavaScript by running:

```
npx tsc
```

This should create a new file named `index.js` in your root folder. We then run this with the command:

```
node index.js
```

#### Response

The console should give a response that looks something like:

```
{ jsonrpc: '2.0', result: '0x789f67', id: 1 }
```

Here we see the RPC node returns:

* The JSON-RPC protocol version, which matches the one we specified.
* The 'result' which in this case is the hexadecimal value representing the current integer block number of Telcoin Network.
* The id, which we had specified as 1 in our request.

If this worked, well done! We've covered some fundamentals of getting started in blockchain development. Now we can look at some more involved requests.

If it didn't, feel free to reach out to one of us in the Telcoin discord.
