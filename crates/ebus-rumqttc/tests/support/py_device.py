"""A python-sdk device tree for the mosquitto end-to-end tests.

The root `py-root` has a node `ctl` with settable properties `text` (string),
`level` (integer) and `on` (boolean); each set_callback calls set_value with
the accepted payload. Children `py-child-00` .. have a node `meter` with integer
properties. Prints `ready` once the tree is built and `set <property> <value>`
for every accepted /set, then runs until it is killed or its stdin closes.
"""

import argparse
import json
import os
import sys

from ebus_sdk import homie


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--children", type=int, default=2)
    parser.add_argument("--props", type=int, default=3)
    args = parser.parse_args()

    root = homie.Device(
        id="py-root", name="Root", type="energy.ebus.device.test", mqtt_cfg={"host": "127.0.0.1", "port": args.port}
    )
    root.start_mqtt_client()

    def settable(node, pid, datatype, value):
        prop = None

        def on_set(v):
            print(f"set {pid} {json.dumps(v)}", flush=True)
            # The callback gets the payload text; set_value takes a bool for boolean.
            prop.set_value(v == "true" if datatype == homie.PropertyDatatype.BOOLEAN else v)

        prop = node.add_property_from_dict(
            {"id": pid, "name": pid, "datatype": datatype, "settable": True, "value": value, "set_callback": on_set}
        )

    with root.state_transition():
        ctl = root.add_node_from_dict({"id": "ctl", "name": "Control", "type": "energy.ebus.capability.test"})
        settable(ctl, "text", homie.PropertyDatatype.STRING, "initial")
        settable(ctl, "level", homie.PropertyDatatype.INTEGER, 1)
        settable(ctl, "on", homie.PropertyDatatype.BOOLEAN, False)
        for i in range(args.children):
            child = homie.Device(id=f"py-child-{i:02d}", name=f"Child {i}", type="energy.ebus.device.test", parent=root)
            meter = child.add_node_from_dict({"id": "meter", "name": "Meter", "type": "energy.ebus.capability.meter"})
            for p in range(args.props):
                meter.add_property_from_dict(
                    {"id": f"p{p}", "name": f"P{p}", "datatype": homie.PropertyDatatype.INTEGER, "value": i * 100 + p}
                )

    print("ready", flush=True)
    sys.stdin.read()
    os._exit(0)


if __name__ == "__main__":
    main()
