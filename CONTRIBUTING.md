# Contributing to rust-sdk

Thanks for your interest in contributing! `rust-sdk` is a Rust SDK for eBus devices and controllers using the [Homie 5 MQTT convention](https://homieiot.github.io/).

## How to contribute

### Discussions

Use [Discussions](https://github.com/electrification-bus/rust-sdk/discussions) for open-ended questions about design, scope or intent, proposed changes to the transport or backend traits, and integration questions that are not yet a clear bug or feature request.

### Issues

Use [Issues](https://github.com/electrification-bus/rust-sdk/issues) for actionable changes: bug reports with reproduction steps (crate, features, target, Rust version, broker, and ideally a failing test or log), concrete feature requests with a use case, and specific documentation gaps.

### Pull requests

Pull requests are welcome.

- For small fixes, open a PR directly. For new public API, changes to the transport or backend traits, or changes to Homie/eBus wire behavior, open a Discussion or Issue first.
- **Spec conformance is the north star.** The [Homie 5 convention](https://homieiot.github.io/) and the [Electrification Bus specification](https://github.com/electrification-bus/specification) define on-the-wire behavior. When the code and the spec disagree, the spec wins.
- **Follow python-sdk where the spec leaves a choice open.** For JSON key order, `json.dumps` formatting, number formatting and the `$description` `version` value, prefer python-sdk's choice unless matching it is costly. Where python-sdk deviates from the spec ([python-sdk#110](https://github.com/electrification-bus/python-sdk/issues/110)), follow the spec.
- **Update `.ebus-spec.json` on every spec sync**: bump `synced_commit`, `synced_date` and `framework`, and edit `supports` to match the framework features the code implements (see the specification's `conventions/spec-provenance.md`).
- **The core crates (`ebus-homie`, `ebus-mqtt`, `ebus-discovery`) must build without `std`.** Gate allocation behind the `alloc` feature and anything else from `std` behind `std`. They perform no I/O and depend on no async runtime.
- **Run the checks CI runs:** `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace`.
- **Respect upstream licenses: reference, don't copy.** This SDK is MIT. Do not paste source from copyleft projects into it.
- **Keep comments to a minimum.** Reserve comments for the non-obvious *why*.
- **Don't hard-wrap Markdown prose.** Write each paragraph as a single line.

## Keep your personal LLM config out of the repo

Do not commit personal LLM coding-assistant config (`CLAUDE.md`, `AGENTS.md`, `.claude/`, `.cursor/`, and so on). Keep it in a separate shadow repo symlinked into your working tree, or add the paths to `.git/info/exclude`. The project `.gitignore` lists the common ones.

## Code of conduct

Be respectful and constructive.

## Maintenance posture

`rust-sdk` is pre-alpha. Updates and maintenance, including responses to issues, happen as time and resources permit. It is maintained alongside the [Electrification Bus specification](https://github.com/electrification-bus/specification), [`python-sdk`](https://github.com/electrification-bus/python-sdk), [`cpp-sdk`](https://github.com/electrification-bus/cpp-sdk) and [`esp32-sdk`](https://github.com/electrification-bus/esp32-sdk).
