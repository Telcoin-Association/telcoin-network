---
description: Hardware requirements for full validator nodes.
---

# Hardware Requirements

## Validator Node

GSMA MNOs have the exclusive right to operate validator nodes and earn network fees. This section is provided to support Telcoin Association's authorized validator node operators. Telcoin (TEL) issuance is provided to Telcoin Network Validators to incentivize the growth of a secure, compliant, efficient financial platform.

#### Purpose of Node Specifications

Minimum hardware, connectivity, and hosting requirements are specified to maintain consistent and efficient performance across Telcoin Network. These ensure that all nodes meet a foundational standard, contributing positively to the overall network's health.

#### Pre-installation Review

Operators are required to submit detailed specifications of their intended node setup to Telcoin Network development team for approval before installation. Please contact our DevOps team at grant@telcoin.org with your proposed hardware specifications.

### CPU Requirements

**Architecture**: x86/x64 compatible

**Minimum Configuration**: 16 cores / 32 threads

**Recommended**: 32 cores - higher clock speed over core count

**Benchmark Scores:** 4000+ Single Thread Performance (PassMark Single Thread)

### Memory

**Minimum**: 128 GB DDR4/DDR5 ECC Registered DIMM (highest MT/s possible)&#x20;

**Recommended**: 128 GB or higher (highest MT/s possible)&#x20;

### Storage

**Minimum**: 4 TB of **TLC** NVMe SSD

**Recommended**: 7.5 TB of **TLC** NVMe SSD

### Networking

**Network Connectivity Minimum**: 1000 Mb/s sustained internet bandwidth, 1-Gigabit Ethernet interface&#x20;

**Recommended**: 1000+ Mb/s sustained internet bandwidth, 10-Gigabit interface

### Operating System

**Supported OS**: Linux LTS Releases Debian 11+Ubuntu 20.04+Red Hat Enterprise Linux (RHEL) 8 Kernel Version: Minimum 3.10+

Please email support@telcoin.org to confirm hardware specifications before purchasing any equipment.



## Observer Node

Observer nodes are permissionless participants in Telcoin Network that allow users to independently verify blockchain state without participating in consensus. These nodes synchronize with the network, maintain a copy of the blockchain, and can submit transactions directly to consensus validators. Unlike validator nodes, they do not validate blocks or participate in the consensus process.

### CPU Requirements

x86 / x64 / ARM64 / AArch64

8 cores / 16 threads (higher clock speed over core count)

### Memory

16-32 GB DDR4 ECC Registered DIMM, 2666MHz or faster

### Storage

500GB - 2TB of **TLC** NVMe SSD (expandable)

### Networking

Stable 24Mbps+ (download speed over upload)

### Operating System

**Supported OS**: Linux LTS Releases Debian 11+Ubuntu 20.04+Red Hat Enterprise Linux (RHEL) 8 Kernel Version: Minimum 3.10+, MacOS Sequoia 15+

#### Observer nodes are ideal for:

* **Developers**: Building applications on Telcoin Network who need reliable, direct access to blockchain data
* **Business integrators**: Companies using Telcoin Network services who want independent verification of transactions and account states
* **Community members**: Enthusiasts who want to support network resilience and independently verify network operations
* **Auditors and researchers**: Organizations requiring transparent access to blockchain data without relying on third-party explorers
* **Application service providers**: Entities offering services built on Telcoin Network who need direct transaction submission capabilities

## Storage Recommendations: Why TLC over QLC

### Understanding NVMe Drive Technology

When setting up nodes for Telcoin Network, storage selection significantly impacts node performance and longevity. We specifically recommend TLC (Triple-Level Cell) NVMe drives over QLC (Quad-Level Cell) options for several important reasons.

### Performance Considerations

TLC drives offer higher sustained read/write speeds and better random I/O performance, which is critical for transaction processing and block validation. They maintain consistent performance during intensive blockchain operations and provide lower latency access to blockchain data. In contrast, QLC drives often suffer from performance degradation when their cache is saturated, causing inconsistent throughput that may lead to validation delays during high transaction volumes.

### Durability and Lifespan

TLC drives typically support 1,000-3,000 P/E (program/erase) cycles, offering 3-5 times higher endurance rating than QLC alternatives. This superior sustained workload handling makes them better suited for continuous blockchain operations. QLC drives generally only support 100-1,000 cycles before cell degradation, and the continuous writes during validation can rapidly deteriorate QLC cells, leading to premature drive failure.

### Blockchain-Specific Requirements

For Telcoin Network nodes, storage drives face continuous write operations when processing incoming transactions, validating new blocks, maintaining state databases, and recording blockchain history. While QLC drives offer upfront cost savings, the performance limitations and shorter lifespan ultimately make TLC drives the more cost-effective and reliable choice for maintaining node stability and network performance over time.

