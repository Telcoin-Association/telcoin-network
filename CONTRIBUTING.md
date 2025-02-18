## Contributing to Telcoin Network protocol

Thank you for your interest in contibuting to Telcoin Network!

All contributions are appreciated and made under both Apache 2.0 and MIT license.

Here are some guidelines to help you get started.
Please reach out on [telcoin-network discord channel](https://discord.com/channels/1252990258514235544/1252996402942836857) if you have any questions.

### Code of Conduct

The Telcoin Network project adheres to the [Rust Code of Conduct](https://www.rust-lang.org/policies/code-of-conduct).
Contributers should reference this code of conduct to ensure they're following the _minimum_ expected behavior in this repo.

Any violation of this Code of Conduct can be reported by emailing [devs@telcoin.org](mailto:devs@telcoin.org).

### Contributions

There are many ways to contribute:

1. **Opening an issue:** Report bugs or suggest enhancements through our issue tracker.
2. **Adding context:** Providing additional context to existing issues, such as examples, screenshots and code snippets, and other details to help resolve issues.
3. **Resolving issues:** Some issues may be resolved by demonstrating that the reported issue is actually not a problem, other times it's necessary to open a pull request.

Please note, the core protocol team has identified and prioritized outstanding issues.
We are tracking these issues on the [Telcoin Network Project board](https://github.com/orgs/Telcoin-Association/projects/2).

**Everyone is welcome to participate as a contributor**.
We want to hear from you by joining the discussion around issues, bugs, and open PRs.

#### Spelling and Grammer Contributions

We do not accept contributions that only address spelling or grammatical errors in the repo.

### Help with your PRs

If you have questions after reviewing the existing documentation or need help with your PR, please post your question to the discussion board.
You can access the discussion board by clicking the "Discussions" tab at the top of the GitHub repo.

Telcoin Network is still under heavy development.
We try our best to maintain documentation for developers and node operators at [docs.telcoin.network](https://docs.telcoin.network).
If you find the documentation insufficient, please post on the discussions board so we can help you and update our docs.

It may take longer for a core member to address your question if it does not directly align with our current roadmap.

### Pull Requests

All code modfiications must be made through pull requests.

Please include an itemized checklist with high-level changes in your PR to help maintainers get an idea of what your PR does.

If you plan to make a large pull request, please open an issue describing the change first.
It's best to coordinate with the core protocol team through discord or in this repo so we can offer feedback and guidance.
Syncing on strategy before opening your PR will increase the likelihod that the PR is merged.
We also recommend opening a WIP Draft PR.

Devs should ensure the following `make` command passes before marking the PR as "ready" for review.

```sh
make pr
```

At this time, core maintainers are the only ones allowed to run the final check required to merge PRs.

#### Adding tests

PRs must include unit tests for any new feature and bug fixes.
In some cases, integration tests (IT) may also be necessary before merging a PR.

We recommend looking through existing tests as a helpful guide to creating new ones.

#### Running tests locally

**make test**: Used by maintainers with credentials to cloud infrastructure.
**make public-test**: All workspace tests that don't require cloud credentials. This is useful for contributers.
**make fmt**: Preffered format checks using nightly rust.
**make clippy**: Prefferred lint check using nightly rust.

#### Commits

There is no limit to the number of commits a PR may have, but we recommend squashing commits that are used as "checkpoint" during development to help maintainers review your PR.

#### Feedback

The core team will most likely provide feedback and request changes to your PR.
It isn't personal, so please don't feel discouraged.
The feedback process is used to ensure consistency from all developers.

Because anyone in the community can review PRs, you may receive a contradictory feedback.
The core protocol team will provide guidance to address conflicting feedback in the PR.

#### Reviewing

Anyone who wishes to review PRs is welcome!

If you choose to review a PR and provide feedback, you have a responsibility to ensure your comments are helpful, respectful, constructive, and insightful.
We ask that you only provide reviews with the explicit goal of bettering the project and providing guidance to the contributor.
If you believe a PR should not be merged, take the time to explain your concerns in great detail so others can understand.

Please be open to feedback and considering different perspectives on solving problems.
Reviews that are disrespectful to the contributor or any other review directly breach the [Code of Conduct](https://www.rust-lang.org/policies/code-of-conduct).

The goal is to work together as an open-source community to develop the best protocol possible.

If we are unable to merge your PR at this time, we hope you will understand that your contribution is still appreciated.

##### Old PRs

It is courteous to check with contributers who have opened PRs that appear to be abandoned or stale before taking it over.
If you take over a PR with a contributor's permission, it's polite to give them credit in your PR.
You can achieve this by keeping their name and email in the commit log.
You can also use `Author: ` and `Co-authored-by: ` in the commit's metadata tag.

_Inspired by [reth's contributing guidelines](https://github.com/paradigmxyz/reth)_
