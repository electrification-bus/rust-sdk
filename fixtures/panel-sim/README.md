# panel-sim reference captures

Masked captures of SPAN panels' `ebus/5` Homie trees, copied unchanged from `ebus_panel_sim/reference_captures/` in the [`ebus-panel-sim`](https://pypi.org/project/ebus-panel-sim/) 0.10.0b3 wheel (`ebus_panel_sim-0.10.0b3-py3-none-any.whl`, SHA-256 `c178ae88935682761cc0d82d24c2e894e692289a6444e330691bf19889cf38df`), from [distribution-enclosure-simulator](https://github.com/electrification-bus/distribution-enclosure-simulator). MIT; see [LICENSE](LICENSE).

| Capture | Panel | Firmware |
|---|---|---|
| `main32_r202633` | MAIN 32 (tree only) | spanos3/r202633/02 |
| `main32_r202639` | MAIN 32 | spanos3/r202639/03 |
| `main32_r202639-upstream-pv` | MAIN 32 with two upstream PV inverters | spanos3/r202639/03 |
| `r202639-a` to `r202639-e` | Edge, MAIN 16, MAIN 40 with Powerwall 3, MLO 24 with battery and Drive, MLO 48 with Drive | spanos3/r202639/03 |

Each `<name>-tree-v1.json` holds the retained tree: per device, the full `$description` and the published property values as strings. `<name>.yaml` is the simulator's panel definition and `<name>.ticks.yaml` 60 one-second ticks, for replay through `ebus-panel-sim`.

Serials, device and circuit ids, addresses, postal codes and Wi-Fi names are masked. Do not add captures from any other source here.
