# Example Style

- Start each example with concise module documentation describing the scenario and the behavior it demonstrates.
- Omit run commands, basic Cargo instructions, and comments that merely restate the code. Assume readers know how to run a Rust example.
- Keep each example focused on a small, coherent scenario. Explain relevant semantic differences beside the code; avoid catalogs of unrelated primitives.
- Keep example explanations in the example source. The repository README only needs a concise entry point.
- Register each example in `datasketches/Cargo.toml` with an explicit `[[example]]` section and its `required-features`, since every sketch feature is opt-in.
