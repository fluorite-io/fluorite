# Fluorite Whitepaper (Typst)

This crate compiles Typst-based whitepapers to PDF using Rust dependencies (`typst-as-lib` + `typst-pdf`).

## Build the eventbus whitepaper PDF

```bash
cargo run -p fluorite-whitepaper
```

Default paths:

- Input: `crates/fluorite-whitepaper/typst/eventbus_whitepaper.typ`
- Output: `crates/fluorite-whitepaper/out/eventbus_whitepaper.pdf`

## Custom paths

```bash
cargo run -p fluorite-whitepaper -- \
  --input crates/fluorite-whitepaper/typst/eventbus_whitepaper.typ \
  --output crates/fluorite-whitepaper/out/eventbus_whitepaper.pdf
```
