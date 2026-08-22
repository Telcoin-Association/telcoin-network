---
description: >-
  Two key technologies, Narwhal and Bullshark, contribute to Telcoin Network's
  consensus layer.
---

# Consensus Layer

Incorporating Bullshark, an asynchronous consensus mechanism, into the blockchain architecture alongside Narwhal's DAG-based mempool represents a forward-thinking approach to distributed ledger technology. Bullshark distinguishes itself by being the first Directed Acyclic Graph (DAG) based asynchronous Byzantine Atomic Broadcast protocol, optimizing for common synchronous cases while ensuring robustness in asynchronous environments.

### Narwhal: The DAG-based Mempool

Narwhal's innovative mempool design uses a Directed Acyclic Graph (DAG) structure for efficient transaction management prior to confirmation. This design choice facilitates a more scalable and efficient transaction dissemination process compared to traditional linear mempools.

#### Key Features:

Parallel Processing: The DAG structure allows for concurrent transaction processing, significantly boosting throughput.

Increased Efficiency: By organizing transactions in a DAG, Narwhal mitigates the inefficiencies of linear mempools, enhancing overall network performance.

Scalability: The design inherently supports scalability, adept at managing high transaction volumes without compromising on performance or resource efficiency.

### Bullshark: Asynchronous Byzantine Atomic Broadcast

Bullshark represents a groundbreaking step in consensus mechanisms, offering a secure and efficient way to achieve consensus in distributed networks. As an asynchronous Byzantine Atomic Broadcast protocol based on a DAG, Bullshark is tailored for high-performance, distributed systems, with optimizations for both synchronous and asynchronous operations.

#### Key Features:

Asynchronous Consensus: Unlike many consensus mechanisms that require some degree of synchrony, Bullshark is designed to work effectively in fully asynchronous environments, providing robustness against timing attacks and network delays.

Optimized for Synchronous Cases: While Bullshark is asynchronous, it is optimized for the more common synchronous cases, ensuring high throughput and low latency during normal operations.

Security in Asynchronous Networks: Bullshark maintains strong security guarantees, even in asynchronous settings, ensuring the integrity and reliability of the consensus process amidst a wide range of network conditions.

### Further Reading

* Danezis, G., Kogias, E. K., Sonnino, A., & Spiegelman, A. (2021). Narwhal and Tusk: A DAG-based Mempool and Efficient BFT Consensus. ArXiv:2105.11827 \[Cs]. [http://arxiv.org/abs/2105.11827](https://web.archive.org/web/20240420204246/http://arxiv.org/abs/2105.11827)
* Spiegelman, A., Giridharan, N., Sonnino, A., & Kokoris-Kogias, L. (2022). Bullshark: DAG BFT Protocols Made Practical. ArXiv:2201.05677 \[Cs]. [https://arxiv.org/pdf/2201.05677](https://arxiv.org/pdf/2201.05677)
