# Development Tools

Now that we have an understanding of interacting with the blockchain, we can start to think about dapp development. There are two main tools used by developers in the web3 space; [Hardhat](https://hardhat.org/) and [Foundry](https://github.com/foundry-rs/foundry).

### Hardhat

Hardhat builds on Ethers.js' functionality and introduces more components which together provide a full development environment. Some of the main highlights include:

* Scripting and testing in TypeScript/ JavaScript, making it useful for integration with front ends.
* Provides the ability to create local forks of networks at previous or current blocks. This allows developers to test their contracts as if they were deployed on Telcoin Network - similar to a sandbox style environment.
* Testing using chai and mocha for clean test code.

These features help Hardhat excel at contract deployment, and integration testing.

Hardhat has a thorough walkthrough and setup available [here](https://hardhat.org/tutorial).

### Foundry

Foundry is an extremely fast testing environment. Contrary to Hardhat, tests are written in solidity (rather than Type/JavaScript) and it also provides advanced testing features such as fuzzing and invariant testing.&#x20;

Foundry has useful documentation on hetting started [here](https://book.getfoundry.sh/getting-started/installation).
