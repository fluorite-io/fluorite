# Flourine Whitepaper (Typst)

This crate compiles Typst-based whitepapers to PDF using Rust dependencies (`typst-as-lib` + `typst-pdf`).

## Build the eventbus whitepaper PDF

```bash
cargo run -p flourine-whitepaper
```

Default paths:

- Input: `crates/flourine-whitepaper/typst/eventbus_whitepaper.typ`
- Output: `crates/flourine-whitepaper/out/eventbus_whitepaper.pdf`

## Custom paths

```bash
cargo run -p flourine-whitepaper -- \
  --input crates/flourine-whitepaper/typst/eventbus_whitepaper.typ \
  --output crates/flourine-whitepaper/out/eventbus_whitepaper.pdf
```
