#!/bin/sh
# Refresh fixtures/panel-sim from a published ebus-panel-sim wheel.
# Usage: tools/sync-panel-sim-captures.sh VERSION SHA256
set -eu
version=$1
sha256=$2
dest=$(cd "$(dirname "$0")/.." && pwd)/fixtures/panel-sim
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
url=$(curl -fsS "https://pypi.org/pypi/ebus-panel-sim/$version/json" |
    python3 -c 'import json,sys; print(next(u["url"] for u in json.load(sys.stdin)["urls"] if u["filename"].endswith(".whl")))')
curl -fsSL -o "$work/sim.whl" "$url"
echo "$sha256  $work/sim.whl" | shasum -a 256 -c -
unzip -q "$work/sim.whl" -d "$work/x"
find "$dest" -maxdepth 1 \( -name '*.json' -o -name '*.yaml' \) -delete
cp "$work"/x/ebus_panel_sim/reference_captures/*.json "$work"/x/ebus_panel_sim/reference_captures/*.yaml "$dest"/
cp "$work"/x/ebus_panel_sim-"$version".dist-info/licenses/LICENSE "$dest"/LICENSE
echo "Copied captures from ebus-panel-sim $version; update the version and SHA-256 in $dest/README.md."
