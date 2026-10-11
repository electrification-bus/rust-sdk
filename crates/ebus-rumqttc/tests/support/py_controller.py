"""A python-sdk controller for the mosquitto end-to-end tests.

Discovers the tree under `--root` and answers one JSON array per stdin line
with one JSON line on stdout:

- ["await", device, values, secs]: waits until the device is ready with its
  description and at least `values` property values; answers
  {"ok": bool, "device": {"state", "description", "properties"}}.
- ["value", device, node, property, payload, secs]: waits until the property
  holds `payload`; answers {"ok": bool, "value": current}.
- ["set", device, node, property, payload]: sends a /set; answers {"ok": bool}.

Prints "ready" once discovery has started; exits when stdin closes.
"""

import argparse
import json
import os
import sys
import time

from ebus_sdk import homie


def dump(device):
    if device is None:
        return None
    return {"state": device.state, "description": device.description, "properties": device.properties}


def wait(cond, secs):
    deadline = time.monotonic() + secs
    while not cond():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.02)
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--root", required=True)
    args = parser.parse_args()

    ctl = homie.Controller(mqtt_cfg={"host": "127.0.0.1", "port": args.port}, root_device_id=args.root)
    ctl.start_discovery()
    print("ready", flush=True)

    def values(device_id):
        device = ctl.get_device(device_id)
        return 0 if device is None else sum(len(v) for v in device.properties.values())

    def complete(device_id, n):
        device = ctl.get_device(device_id)
        return (
            device is not None and device.description is not None and device.state == "ready" and values(device_id) >= n
        )

    def current(device_id, node, prop):
        device = ctl.get_device(device_id)
        return None if device is None else device.get_property(node, prop)

    for line in sys.stdin:
        cmd = json.loads(line)
        if cmd[0] == "await":
            _, device_id, n, secs = cmd
            ok = wait(lambda: complete(device_id, n), secs)
            reply = {"ok": ok, "device": dump(ctl.get_device(device_id))}
        elif cmd[0] == "value":
            _, device_id, node, prop, payload, secs = cmd
            ok = wait(lambda: current(device_id, node, prop) == payload, secs)
            reply = {"ok": ok, "value": current(device_id, node, prop)}
        elif cmd[0] == "set":
            _, device_id, node, prop, payload = cmd
            reply = {"ok": ctl.set_property(device_id, node, prop, payload)}
        else:
            reply = {"ok": False, "error": f"unknown command {cmd[0]}"}
        print(json.dumps(reply), flush=True)
    os._exit(0)


if __name__ == "__main__":
    main()
