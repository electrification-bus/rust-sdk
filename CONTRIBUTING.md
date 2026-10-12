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

## LLM coding assistants

[`AGENTS.md`](AGENTS.md) holds the shared instructions for LLM coding assistants (Codex, Cursor, GitHub Copilot and others read it; [`CLAUDE.md`](CLAUDE.md) imports it for Claude Code). Change it by pull request when the project's conventions change.

Keep your personal configuration out of the repo: `CLAUDE.local.md` (which Claude Code loads alongside `CLAUDE.md`), `.claude/`, `.cursor/` and the like are gitignored. To version them anyway, keep them in a separate private "shadow" repository and symlink them into your working tree, for example `ln -s ~/path/to/shadow-repo/rust-sdk/CLAUDE.local.md CLAUDE.local.md`; for a file you don't need to version, `.git/info/exclude` works too.

## Releasing

The six crates share the workspace version and are published together, in dependency order: `ebus-mqtt`, `ebus-homie`, `ebus-discovery`, `ebus-homie-tokio`, `ebus-discovery-tokio`, `ebus-rumqttc`.

### One-time setup

1. Publish each crate once from a laptop, with a personal crates.io API token that has the `publish-new` and `publish-update` scopes (crates.io requires a verified email address). From a clean checkout of the release commit: `cargo publish --workspace --locked` publishes all six in dependency order. If it stops partway, publish the rest with `cargo publish -p <crate> --locked`, in the order above.
2. Add the GitHub team as an owner of each crate: `cargo owner --add github:electrification-bus:core-contributors <crate>`. Your GitHub account must belong to the team, and the crates.io application must have access to the `electrification-bus` organization. A team owner can publish and yank versions but cannot add or remove owners; that stays with the user owners.
3. On crates.io, for each crate, open Settings, then Trusted Publishing, and add a GitHub publisher: owner `electrification-bus`, repository `rust-sdk`, workflow `release.yml`, environment `release`.
4. In the GitHub repository settings, the `release` environment can restrict which tags may deploy and require a reviewer before publishing.

### Later releases

1. Bump `version` in `[workspace.package]` and in each version requirement between the crates (`[workspace.dependencies]`, `ebus-rumqttc`'s dependency on `ebus-homie-tokio`, and `tools/guide-doctest`'s on `ebus-discovery-tokio` and `ebus-rumqttc`). Then run `cargo update --workspace` and commit the updated `Cargo.lock` with the bump.
2. Move the `[Unreleased]` entries in `CHANGELOG.md` under the new version and its date.
3. Merge, then tag the merge commit and push the tag: `git tag v0.2.0 && git push origin v0.2.0`.

The tag starts `.github/workflows/release.yml`, which runs the CI suite, checks that the tag matches the workspace version, and publishes the six crates with a short-lived token from trusted publishing. It skips any crate whose version is already on crates.io, so if it fails partway, re-run it.

## Code of conduct

Be respectful and constructive.

## Maintenance posture

`rust-sdk` is pre-alpha. Updates and maintenance, including responses to issues, happen as time and resources permit. It is maintained alongside the [Electrification Bus specification](https://github.com/electrification-bus/specification), [`python-sdk`](https://github.com/electrification-bus/python-sdk), [`cpp-sdk`](https://github.com/electrification-bus/cpp-sdk) and [`esp32-sdk`](https://github.com/electrification-bus/esp32-sdk).
