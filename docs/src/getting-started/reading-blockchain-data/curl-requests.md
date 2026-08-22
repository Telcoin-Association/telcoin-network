---
description: We can query the blockchain instantly using cURL (Client URL) requests
---

# cURL requests

Certain nodes running the Telcoin Network have a Remote Procedure Call (RPC) endpoint. These endpoints are a type of Application Programming Interface (API) which allows users to execute a procedure ( a piece of code) on the network as if it occurred locally.&#x20;

RPCs have a few key components:

* Client - The system initiating the RPC request - i.e. the user/ developer's machine
* Server - The system that receives and processes the RPC request - i.e. the Telcoin Network node.
* Protocol - Defines the rules and conventions for data exchange between the client and server - i.e. the Telcoin network source code running on the Telcoin Network node.

#### Using cURL to Make RPC Calls

cURL is a command-line tool used to transfer data to and from a server using various protocols, including HTTP, HTTPS, FTP, and more. In the context of RPC, cURL can be used to send HTTP POST requests to an RPC server and receive responses. This is what we will demonstrate here.

{% hint style="warning" %}
All commands throughout the documentation will be focused on use with Unix-like (e.g. macOS, WSL2) systems.
{% endhint %}

### eth\_blockNumber

The [eth\_blockNumber](../../rpc-methods/eth\_blocknumber.md) method is one of the simplest RPC calls to make. From the [documentation](curl-requests.md#eth\_blocknumber) we see it takes no arguments (parameters) - the 'parameters' section shows `None`, and it returns one value representing the current block number of the network.

#### Request

To query this method, we can simply copy the code from the 'Request' section of the page:

```
curl https://rpc.adiri.tel \
-X POST \
-H "Content-Type: application/json" \
--data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":83}'
```

Let's break down this command:

* `curl https://rpc.adiri.tel`: Specifies the URL of the Ethereum RPC endpoint. This is the URL that one of the Telcoin Network Adiri Testnet nodes is accessible from.
* `-X POST`: Indicates that the request is a POST request.
* `-H "Content-Type: application/json"`: Sets the content type of the request to JSON.
* `--data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":83}'`: Provides the JSON data for the request. This JSON object includes:
  * `"jsonrpc": "2.0"`: The version of the JSON-RPC protocol being used.
  * `"method": "eth_blockNumber"`: The RPC method being called.
  * `"params": []`: An empty array, as this method does not require parameters.
  * `"id": 83`: An arbitrary identifier for the request. This could be any numerical value.

#### Executing the command

paste the above command into your command line and press 'Enter'. You should receive a response.

#### Response

The response should look something like:

```
{
"jsonrpc":"2.0",
"result":"0x7328cb", //7547083
"id":83
}
```

Note:&#x20;

* `"jsonrpc": "2.0"`: Confirms the JSON-RPC protocol version.
* `"result": "0x7328cb"`: The hexadecimal value of the current block number.
* `"id": 83`: The ID of the request, matching the one sent.



