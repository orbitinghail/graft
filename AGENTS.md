# Agent Contribution Guide

## Tools

When changing rust code:

- run tests using `just test` or `cargo nextest run <specific test>`
- run lints using `cargo clippy`
- ensure the code is formatted correctly using `cargo fmt`

When changing docs (in ./docs):

- verify the docs build using `pnpm build` from within the docs directory

## Coding conventions

Graft is low-level systems software. Prioritize safety, performance, and clarity above all. Follow these principles which are based on [TigerStyle]:

[TigerStyle]: https://tigerstyle.dev/

**Safety:**

- Control flow: Use simple, explicit control structures. Avoid recursion. Keep functions under 70 lines. Centralize branching logic in parent functions.
- Memory & types: Use fixed-size types (e.g. u32, i64). Prefer to allocate memory at startup or make use of the stack. Avoid dynamically checked borrow rules (e.g. `RefCell`) and `dyn` usage.
- Error handling: Use assertions for invariants and argument checks. Treat warnings as errors.

**Performance:**

- Early design: Apply napkin math to estimate bottlenecks. Design for performance from the start.
- Batching: Batch I/O or expensive operations. Prioritize optimizing network > disk > memory > CPU.
- Predictability: Write predictable, branch-friendly code. Don't rely on compiler optimizations.

**Clarity:**

- Naming: Use clear variable names. Avoid abbreviations and single-letter variable names. Use specific types like ByteUnit and Duration rather than bare types for variables that have logical units.
- Structure: Keep functions simple. Group related code. Declare variables near usage.
- Consistency: Avoid aliases/dupes. Pass large values by reference. Maintain consistent indentation, comment style, and toolchain. Write idiomatic Rust code.
- Off-by-One safety: Treat indexes, counts, sizes as distinct. Be explicit in rounding and division.

## Development and testing instructions

- Run tests using `just test`
- To focus on one crate, use `cargo nextest run -p <crate>`
- To focus on one test, use `cargo nextest run <test>`
- Fix any test or type errors until the whole suite is green.
- Add or update tests for the code you change, even if nobody asked.
