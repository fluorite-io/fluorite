# Turbine Whitepaper (Typst)

This crate compiles Typst-based whitepapers to PDF using Rust dependencies (`typst-as-lib` + `typst-pdf`).

## Build the eventbus whitepaper PDF

```bash
cargo run -p turbine-whitepaper
```

Default paths:

- Input: `crates/turbine-whitepaper/typst/eventbus_whitepaper.typ`
- Output: `crates/turbine-whitepaper/out/eventbus_whitepaper.pdf`

## Custom paths

```bash
cargo run -p turbine-whitepaper -- \
  --input crates/turbine-whitepaper/typst/eventbus_whitepaper.typ \
  --output crates/turbine-whitepaper/out/eventbus_whitepaper.pdf
```
