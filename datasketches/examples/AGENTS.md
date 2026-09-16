# Examples

Examples are runnable, end-to-end scenarios that demonstrate a sketch's semantics in a realistic setting.

- Never write "Run with: `cargo run --example ...`" lines or other usage instructions in example files. Cargo's example discovery is the interface; per-file run commands are noise and go stale.
- Prefer a concrete business scenario over an abstract type-level demo. State which guarantee is exact in that setting and what the probabilistic caveats cost, so the example teaches when the operation applies.
- Register each example in `datasketches/Cargo.toml` with an explicit `[[example]]` section and its `required-features`, since every sketch feature is opt-in.
