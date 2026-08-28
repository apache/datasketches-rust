# Repository instructions for agents

Before planning or modifying this repository, read [CONTRIBUTING.md](CONTRIBUTING.md) in full and treat its build, test, integration-test layout, and serialization snapshot guidance as repository requirements.

For test changes, pay particular attention to the "Integration test layout" and "Serialization snapshots" sections. Keep the documented workflow synchronized with structural changes, and run the applicable `cargo x check`, `cargo x test`, and `cargo x lint` commands before handing work back.

Apply the changelog guidance in [CONTRIBUTING.md](CONTRIBUTING.md) to every change. Update the permanent `Unreleased` section in the same pull request for significant user-visible behavior, and do not add entries mechanically for excluded maintenance work.

Treat `CHANGELOG.md` as release notes for users rather than a summary of implementation work. Name the affected API or workload and the observable outcome, and keep performance claims within the scenario supported by evidence.
