# Networks and RPC Endpoints

> [!NOTE]
> The public RPC URLs listed below are rate limited and do not support websocket connections: they are load balanced, and the balancer answers a WebSocket upgrade with HTTP 405. If such functionality is required, please reach out to the team.

## Adiri Testnet

<table><thead><tr><th width="220">Parameter</th><th>Value</th></tr></thead><tbody><tr><td>Network Name</td><td>Telcoin Network Adiri Testnet</td></tr><tr><td>Chain ID</td><td><code>0x7e1</code> (2017)</td></tr><tr><td>Currency Symbol</td><td>TEL</td></tr><tr><td>Explorers</td><td><a href="https://telscan.io/">https://telscan.io/</a>, <a href="https://www.telscan.xyz/">https://www.telscan.xyz/</a></td></tr><tr><td>Public RPC URL</td><td><a href="https://rpc.adiri.tel">https://rpc.adiri.tel</a></td></tr></tbody></table>

### Unstable Testnet

Adiri Testnet is the only network currently deployed. While Adiri Testnet is intended to be as stable as possible, the protocol is still under heavy development and may be reset from time to time before mainnet is launched to incorporate breaking changes.

### Developer Notes

The public RPC endpoint at https://rpc.adiri.tel uses a load balancer and is primarily useful for single transactions and read queries.

Developers should prefer to connect directly with a node when running scripts or multiple consecutive transactions. Finality happens after a few seconds (up to 10s on the current testnet), and some nodes may not have updated state until then.

Developers should use [Chainlist](https://chainlist.org/?testnets=true&search=adiri) (include testnets and search "adiri") for an up-to-date list of supported RPC nodes for public use. For more developer questions, please email support@telcoin.org.

## Mainnet

Mainnet has not launched. It will use chain ID `0x1e7` (487) and the public RPC URL https://rpc.telcoin.network. That hostname does not serve mainnet before launch, so check that `eth_chainId` returns `0x1e7` before sending anything through it.
