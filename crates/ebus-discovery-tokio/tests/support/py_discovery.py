"""python-service-discovery for the interop tests.

advertise IFACE SERVER ADDRESS TYPE PORT DEVICE_ID: advertise one broker
under SERVER with ADDRESS, print "ready <instance> <server>", and withdraw
when stdin closes. SERVER "-" is the OS responder's host name, with no
address records; ADDRESS "-" is none.

find IFACE URL: run find_broker in discovery-with-fallback with URL as the
configured broker and print the endpoint as JSON.

IFACE is an interface name or address, or "all".
"""

import json
import sys

from ebus_service_discovery import mdns
from ebus_service_discovery.ebus import BrokerService, Identity


def advertise(iface, server, address, service_type, port, device_id):
    identity = Identity(
        device_ids=[device_id],
        roles=["broker-host"],
        manufacturer="Acme",
        model="P1",
        serial_number="SN1",
    )
    adv = mdns.Advertiser(
        identity,
        brokers=BrokerService(service_type=service_type, port=int(port)),
        server=None if server == "-" else server,
        addresses=None if address == "-" else [address],
        interfaces=iface,
        interface_check_interval=None,
    ).start()
    print("ready", adv.instance_name, adv.server, flush=True)
    sys.stdin.read()
    adv.stop()


def find(iface, url):
    ep = mdns.find_broker("discovery-with-fallback", url, interfaces=iface)
    print(
        json.dumps(
            {
                "service_type": ep.service_type,
                "host": ep.host,
                "port": ep.port,
                "server": ep.server,
                "instance_name": ep.instance_name,
                "device_id": ep.txt.get("device_id"),
                "url": ep.url,
                "addresses": sorted(
                    a.address for a in ep.addresses if a.is_usable_candidate and a.family.name == "IPV4"
                ),
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    cmd, *args = sys.argv[1:]
    {"advertise": advertise, "find": find}[cmd](*args)
