#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 Clark Communications Corporation
"""Generate the cross-language test vectors under rust-sdk/vectors/.

Spec suites (topics, empty-string, effective-state, payloads/*, description,
check-value) express the Homie 5 convention. Where python-sdk has a matching
function its result is the expect value, except for the cases in
CONVENTION_OVERRIDES, where python-sdk deviates from the convention
(python-sdk#110) and the override's expect and source replace it. Suites with no python-sdk function (topics,
description, effective-state) are derived from convention.md and their
"source" says so.

python-sdk suites (vectors/python-sdk/) record python-sdk's behavior where the
convention leaves a choice open; every expect value there is computed by
calling python-sdk. float32-format is the exception: it records rust-sdk's f32
choice, numpy's shortest float32 digits laid out by python-sdk's
_format_float.

Run from the eBus repo root with the SDK on PYTHONPATH:

    PYTHONPATH=python-sdk/src:ebus-mqtt-client/src:python-service-discovery/src \
        python3 rust-sdk/tools/gen_vectors.py

Output is deterministic: running twice produces byte-identical files.
"""

from __future__ import annotations

import datetime
import json
import logging
import math
import os
import random
import struct
import sys
from fractions import Fraction  # noqa: F401  (documents the oracle's arithmetic)

import numpy as np

logging.disable(logging.CRITICAL)

import ebus_sdk  # noqa: E402
from ebus_sdk import homie  # noqa: E402
from ebus_sdk.homie import (  # noqa: E402
    Device,
    DeviceState,
    Node,
    Property,
    PropertyDatatype,
    _encode_duration,
    _format_float,
    _parse_set_payload,
    decode_empty_string,
    encode_empty_string,
    sanitize_homie_id,
)

OUT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "vectors")
GENERATOR = f"rust-sdk tools/gen_vectors.py from python-sdk {ebus_sdk.__version__}"
SCHEMA = "https://ebus.energy/schemas/conformance-vectors.json"
CONV = "homie-5:convention.md"
PY = "python-sdk:homie.py"
ISSUE = "python-sdk#110"
PYSDK = "python-sdk"  # directory and suite-name prefix of the python-sdk suites


# ── helpers ────────────────────────────────────────────────────────────────


def write_suite(rel_path: str, suite: str, cases: list) -> None:
    ids = [c["id"] for c in cases]
    dupes = sorted({i for i in ids if ids.count(i) > 1})
    if dupes:
        raise SystemExit(f"{rel_path}: duplicate case ids {dupes}")
    doc = {"$schema": SCHEMA, "suite": suite, "version": "0.1", "generator": GENERATOR, "cases": cases}
    path = os.path.join(OUT, rel_path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        # allow_nan=False: a vector file must be strict JSON.
        f.write(json.dumps(doc, indent=2, ensure_ascii=False, allow_nan=False))
        f.write("\n")
    COUNTS.append((rel_path, len(cases)))


COUNTS: list = []


def payload_field(text: str) -> dict:
    """`payload` for ordinary text, `payload_hex` for zero-length or 0x00-bearing payloads."""
    if text == "" or "\x00" in text:
        return {"payload_hex": text.encode("utf-8").hex()}
    return {"payload": text}


def set_result(text: str, datatype: str, fmt, current) -> dict:
    value, problem = _parse_set_payload(text, datatype, fmt, current)
    if problem:
        return {"problem": problem}
    return {"ok": value}


# ── topics (spec, from convention.md) ──────────────────────────────────────

LOG_LEVELS = ("debug", "info", "warn", "error", "fatal")


def topic_ok(domain, device, kind, **fields) -> dict:
    out = {"valid": True, "domain": domain, "device": device, "kind": kind}
    out.update(fields)
    return out


def topic_bad(hint: str) -> dict:
    return {"valid": False, "error": hint}


def gen_topics() -> list:
    src_ids = f"{CONV}#topic-ids (convention.md:36-42), derived from the convention: python-sdk has no topic parser"
    src_dev = f"{CONV}#device-attributes (convention.md:190-200), derived from the convention"
    src_prop = f"{CONV}#property-attributes, #property-command-topic, derived from the convention"
    src_log = f"{CONV}#logging (convention.md:549-562), derived from the convention"
    src_alert = f"{CONV}#alert-topic (convention.md:509-522), derived from the convention"
    src_bc = f"{CONV}#broadcast-topic (convention.md:524-540), derived from the convention"
    src_root = f"{CONV}#domain-and-root-topic (convention.md:152-157), derived from the convention"
    rows = [
        # (id, topic, expect, source)
        ("state", "homie/5/dev/$state", topic_ok("homie", "dev", "DeviceState"), src_dev),
        ("state-ebus-domain", "ebus/5/panel-1/$state", topic_ok("ebus", "panel-1", "DeviceState"), src_root),
        ("description", "homie/5/dev/$description", topic_ok("homie", "dev", "DeviceDescription"), src_dev),
        ("device-id-digits", "homie/5/000010024176/$state", topic_ok("homie", "000010024176", "DeviceState"), src_ids),
        ("device-id-hyphens", "homie/5/-a--b-/$state", topic_ok("homie", "-a--b-", "DeviceState"), src_ids),
        ("custom-domain", "my-domain-2/5/dev/$state", topic_ok("my-domain-2", "dev", "DeviceState"), src_root),
        ("property-value", "homie/5/dev/node/prop", topic_ok("homie", "dev", "PropertyValue", node="node", property="prop"), src_prop),
        ("property-set", "homie/5/dev/node/prop/set", topic_ok("homie", "dev", "PropertySet", node="node", property="prop"), src_prop),
        ("property-target", "homie/5/dev/node/prop/$target", topic_ok("homie", "dev", "PropertyTarget", node="node", property="prop"), src_prop),
        ("property-named-set", "homie/5/dev/node/set", topic_ok("homie", "dev", "PropertyValue", node="node", property="set"), src_prop),
        ("node-named-set", "homie/5/dev/set/prop/set", topic_ok("homie", "dev", "PropertySet", node="set", property="prop"), src_prop),
        ("alert", "homie/5/dev/$alert/battery", topic_ok("homie", "dev", "DeviceAlert", alert_id="battery"), src_alert),
        ("alert-digits", "homie/5/dev/$alert/0", topic_ok("homie", "dev", "DeviceAlert", alert_id="0"), src_alert),
        ("broadcast-one", "homie/5/$broadcast/alert", topic_ok("homie", None, "Broadcast", subtopic="alert"), src_bc),
        ("broadcast-multi", "homie/5/$broadcast/security/alert", topic_ok("homie", None, "Broadcast", subtopic="security/alert"), src_bc),
        ("broadcast-deep", "ebus/5/$broadcast/a/b/c-1", topic_ok("ebus", None, "Broadcast", subtopic="a/b/c-1"), src_bc),
    ]
    for level in LOG_LEVELS:
        rows.append((f"log-{level}", f"homie/5/dev/$log/{level}", topic_ok("homie", "dev", "DeviceLog", level=level), src_log))
    bad = [
        ("version-4", "homie/4/dev/$state", "badVersion", src_root),
        ("version-50", "homie/50/dev/$state", "badVersion", src_root),
        ("version-5-0", "homie/5.0/dev/$state", "badVersion", src_root),
        ("version-missing", "homie//dev/$state", "badVersion", src_root),
        ("domain-upper", "Homie/5/dev/$state", "invalidDomain", src_root),
        ("domain-empty", "/5/dev/$state", "invalidDomain", src_root),
        ("domain-dollar", "$homie/5/dev/$state", "invalidDomain", src_root),
        ("domain-underscore", "my_domain/5/dev/$state", "invalidDomain", src_root),
        ("device-upper", "homie/5/Dev/$state", "invalidDeviceId", src_ids),
        ("device-underscore", "homie/5/my_dev/$state", "invalidDeviceId", src_ids),
        ("device-dot", "homie/5/my.dev/$state", "invalidDeviceId", src_ids),
        ("device-space", "homie/5/my dev/$state", "invalidDeviceId", src_ids),
        ("device-unicode", "homie/5/dév/$state", "invalidDeviceId", src_ids),
        ("device-dollar", "homie/5/$dev/$state", "invalidDeviceId", src_ids),
        ("device-plus-wildcard", "homie/5/+/$state", "invalidDeviceId", src_ids),
        ("device-hash-wildcard", "homie/5/#", "invalidDeviceId", src_ids),
        ("device-empty", "homie/5//$state", "invalidDeviceId", src_ids),
        ("too-short-root", "homie/5", "tooShort", src_root),
        ("too-short-root-slash", "homie/5/", "invalidDeviceId", src_root),
        ("device-base-topic", "homie/5/dev", "tooShort", src_dev),
        ("empty-topic", "", "invalidDomain", src_root),
        ("leading-slash", "/homie/5/dev/$state", "invalidDomain", src_root),
        ("trailing-slash", "homie/5/dev/$state/", "unexpectedLevel", src_dev),
        ("unknown-attribute", "homie/5/dev/$name", "unknownAttribute", src_dev),
        ("unknown-attribute-homie3", "homie/5/dev/$homie", "unknownAttribute", src_dev),
        ("attribute-upper", "homie/5/dev/$STATE", "unknownAttribute", src_dev),
        ("state-extra-level", "homie/5/dev/$state/x", "unexpectedLevel", src_dev),
        ("description-extra-level", "homie/5/dev/$description/x", "unexpectedLevel", src_dev),
        ("log-no-level", "homie/5/dev/$log", "missingLogLevel", src_log),
        ("log-unknown-level", "homie/5/dev/$log/trace", "unknownLogLevel", src_log),
        ("log-level-upper", "homie/5/dev/$log/WARN", "unknownLogLevel", src_log),
        ("log-subtopic", "homie/5/dev/$log/warn/x", "unexpectedLevel", src_log),
        ("alert-no-id", "homie/5/dev/$alert", "missingAlertId", src_alert),
        ("alert-empty-id", "homie/5/dev/$alert/", "invalidAlertId", src_alert),
        ("alert-dollar-id", "homie/5/dev/$alert/$x", "invalidAlertId", src_alert),
        ("alert-upper-id", "homie/5/dev/$alert/Battery", "invalidAlertId", src_alert),
        ("alert-subtopic", "homie/5/dev/$alert/a/b", "unexpectedLevel", src_alert),
        ("node-base-topic", "homie/5/dev/node", "tooShort", src_prop),
        ("node-dollar", "homie/5/dev/$node/prop", "unknownAttribute", src_prop),
        ("node-upper", "homie/5/dev/Node/prop", "invalidNodeId", src_ids),
        ("node-empty", "homie/5/dev//prop", "invalidNodeId", src_ids),
        ("property-upper", "homie/5/dev/node/Prop", "invalidPropertyId", src_ids),
        ("property-dollar", "homie/5/dev/node/$prop", "invalidPropertyId", src_ids),
        ("property-empty", "homie/5/dev/node/", "invalidPropertyId", src_ids),
        ("property-unknown-suffix", "homie/5/dev/node/prop/get", "unexpectedLevel", src_prop),
        ("property-set-upper", "homie/5/dev/node/prop/SET", "unexpectedLevel", src_prop),
        ("property-dollar-set", "homie/5/dev/node/prop/$set", "unexpectedLevel", src_prop),
        ("property-unknown-attribute", "homie/5/dev/node/prop/$format", "unexpectedLevel", src_prop),
        ("property-set-extra", "homie/5/dev/node/prop/set/x", "unexpectedLevel", src_prop),
        ("property-target-extra", "homie/5/dev/node/prop/$target/x", "unexpectedLevel", src_prop),
        ("broadcast-no-subtopic", "homie/5/$broadcast", "missingSubtopic", src_bc),
        ("broadcast-empty-subtopic", "homie/5/$broadcast/", "invalidSubtopic", src_bc),
        ("broadcast-empty-segment", "homie/5/$broadcast/a//b", "invalidSubtopic", src_bc),
        ("broadcast-trailing-slash", "homie/5/$broadcast/a/", "invalidSubtopic", src_bc),
        ("broadcast-upper", "homie/5/$broadcast/Alert", "invalidSubtopic", src_bc),
        ("broadcast-dollar-segment", "homie/5/$broadcast/$x", "invalidSubtopic", src_bc),
        ("broadcast-wildcard", "homie/5/$broadcast/#", "invalidSubtopic", src_bc),
    ]
    cases = [{"id": f"topic-{i}", "source": s, "input": {"topic": t}, "expect": e} for i, t, e, s in rows]
    cases += [{"id": f"topic-bad-{i}", "source": s, "input": {"topic": t}, "expect": topic_bad(h)} for i, t, h, s in bad]
    return cases


# ── empty-string (spec; computed from encode/decode_empty_string and _parse_set_payload) ──


def gen_empty_string() -> list:
    src = f"{CONV}#empty-string-values (convention.md:60-72); {PY} decode_empty_string/encode_empty_string"
    cases = []
    samples = [("nul", "\x00"), ("text", "A"), ("two-nul", "\x00\x00"), ("nul-then-text", "\x00a"), ("space", " ")]
    for where in ("value", "set", "target"):
        cases.append(
            {
                "id": f"empty-{where}-zero-length-deletes",
                "source": f"{CONV}#empty-string-values (convention.md:60-61), derived from the convention",
                "input": {"direction": "decode", "topic_kind": where, "datatype": "string", "payload_hex": ""},
                "expect": {"deleted": True},
            }
        )
        for label, text in samples:
            cases.append(
                {
                    "id": f"empty-{where}-{label}",
                    "source": src,
                    "input": {"direction": "decode", "topic_kind": where, "datatype": "string", **payload_field(text)},
                    "expect": {"deleted": False, "value": decode_empty_string(text)},
                }
            )
    for where in ("value", "set", "target"):
        cases.append(
            {
                "id": f"empty-{where}-invalid-utf8",
                "source": f"{CONV}#payloads (convention.md:76), derived from the convention",
                "input": {"direction": "decode", "topic_kind": where, "datatype": "string", "payload_hex": "ff"},
                "expect": {"error": "invalidUtf8"},
            }
        )
    for label, value in [("empty", ""), ("text", "A"), ("space", " "), ("nul", "\x00")]:
        enc = encode_empty_string(value)
        cases.append(
            {
                "id": f"empty-encode-{label}",
                "source": src,
                "input": {"direction": "encode", "datatype": "string", "value": value},
                "expect": {"payload_hex": enc.encode("utf-8").hex()},
            }
        )
    # 0x00 is the empty string only for string; other datatypes reject it (convention.md:65-67).
    for dt, fmt in [("integer", None), ("float", None), ("boolean", None), ("enum", "a,b"), ("color", "rgb"),
                    ("datetime", None), ("duration", None), ("json", None)]:
        inp = {"direction": "set", "datatype": dt, "payload_hex": "00"}
        if fmt is not None:
            inp["format"] = fmt
        value, problem = _parse_set_payload("\x00", dt, fmt, None)
        assert problem, (dt, value)
        cases.append(
            {
                "id": f"empty-set-nul-{dt}-rejected",
                "source": f"{CONV}#empty-string-values (convention.md:65-67); {PY} _parse_set_payload",
                "input": inp,
                "expect": {"valid": False, "problem": problem},
            }
        )
    return cases


# ── effective-state (spec, from the convention table) ─────────────────────


def gen_effective_state() -> list:
    # convention.md:262-267: only a root in "lost" overrides the device's own
    # $state. python-sdk's HOMIE_EFFECTIVE_STATE_TABLE also propagates init,
    # disconnected and sleeping from the root (python-sdk#110 item 1).
    src = (
        f"{CONV}#device-hierarchy (convention.md:262-267), derived from the convention; "
        f"{ISSUE} item 1: python-sdk also propagates a root's init, disconnected and sleeping"
    )
    states = [s.value for s in DeviceState]
    cases = []
    for own in states:
        cases.append(
            {
                "id": f"effective-no-root-{own}",
                "source": src,
                "input": {"has_root": False, "root_state": None, "own_state": own},
                "expect": {"state": own},
            }
        )
    for root in states:
        for own in states:
            eff = "lost" if root == "lost" else own
            cases.append(
                {
                    "id": f"effective-root-{root}-own-{own}",
                    "source": src,
                    "input": {"has_root": True, "root_state": root, "own_state": own},
                    "expect": {"state": eff},
                }
            )
    return cases


# ── payloads/<datatype> (spec; computed from _parse_set_payload) ──────────

PAYLOAD_CASES = {
    "integer": [
        ("zero", "0", None), ("neg-zero", "-0", None), ("positive", "42", None), ("negative", "-42", None),
        ("leading-zeros", "007", None), ("neg-leading-zeros", "-007", None), ("lone-minus", "-", None),
        ("zero-length", "", None), ("plus-sign", "+1", None), ("leading-space", " 1", None),
        ("trailing-space", "1 ", None), ("trailing-newline", "1\n", None), ("decimal", "1.0", None),
        ("exponent", "1e3", None), ("int64-max", "9223372036854775807", None),
        ("int64-max-plus-1", "9223372036854775808", None), ("int64-min", "-9223372036854775808", None),
        ("int64-min-minus-1", "-9223372036854775809", None), ("huge", "1" + "0" * 40, None),
        ("hex", "0x1F", None), ("underscore", "1_000", None), ("double-minus", "--1", None),
        ("fullwidth-digits", "１２", None), ("arabic-indic-digit", "٣", None),
        ("nan", "nan", None), ("inf", "inf", None),
        ("range-in", "10", "0:10"), ("range-min", "0", "0:10"), ("range-above", "11", "0:10"),
        ("range-below", "-1", "0:10"), ("open-max", "1000000", "0:"), ("open-min", "-1000000", ":10"),
        ("step-rounds-up", "5", "0:10:2"), ("step-from-max", "5", ":10:2"), ("step-exact", "4", "0:10:2"),
        ("step-above-after-round", "11", "0:10:2"), ("step-not-whole", "3", "0:10:2.5"),
        ("step-whole-from-fraction", "4", "0:10:2.5"), ("leading-zeros-step-changes", "007", "0:10:2"),
        ("leading-zeros-step-unchanged", "008", "0:10:2"), ("format-unparseable", "5", "a:b"),
        ("format-too-many-parts", "5", "1:2:3:4"), ("step-zero-ignored", "5", "0:10:0"),
        ("step-negative-ignored", "5", "0:10:-2"),
        ("int64-max-step-overflow", "9223372036854775807", "0::2"),
        ("int64-max-step-in-range", "9223372036854775806", "0::2"),
    ],
    "float": [
        ("zero", "0", None), ("neg-zero", "-0", None), ("zero-point-zero", "0.0", None), ("neg-zero-float", "-0.0", None),
        ("integer", "1", None), ("negative", "-1", None), ("decimal", "1.5", None), ("leading-dot", ".5", None),
        ("trailing-dot", "5.", None), ("neg-leading-dot", "-.5", None), ("exponent", "1e5", None),
        ("exponent-upper", "1E5", None), ("exponent-negative", "1e-5", None), ("exponent-plus", "1e+5", None),
        ("leading-zeros", "00012.5", None), ("lone-minus", "-", None), ("zero-length", "", None),
        ("plus-sign", "+1", None), ("leading-space", " 1", None), ("trailing-space", "1 ", None),
        ("two-dots", "1.2.3", None), ("lone-dot", ".", None), ("exponent-no-digits", "1e", None),
        ("exponent-only", "e5", None), ("exponent-fraction", "1e5.5", None), ("dot-exponent", ".e5", None),
        ("inf", "inf", None), ("infinity", "Infinity", None), ("nan", "nan", None), ("nan-upper", "NaN", None),
        ("max", "1.7976931348623157e308", None), ("above-max-overflows", "1e309", None), ("1e999", "1e999", None),
        ("neg-1e999", "-1e999", None), ("underflow-to-zero", "1e-400", None), ("min-subnormal", "4.9e-324", None),
        ("hex", "0x10", None), ("underscore", "1_000.5", None), ("comma-decimal", "1,5", None),
        ("double-minus", "--1", None), ("fullwidth-digit", "１", None),
        ("range-in", "5.5", "0:10"), ("range-above", "10.0001", "0:10"), ("range-below", "-0.1", "0:10"),
        ("range-max-inclusive", "10", "0:10"), ("float-bounds", "10.2", "10.123:15.123"),
        ("step-half", "0.3", "0:10:0.5"), ("step-half-tie-up", "2.25", "0:10:0.5"),
        ("step-exact-unchanged", "2.5", "0:10:0.5"), ("step-tenth", "0.35", "0:1:0.1"),
        ("step-result-needs-reformat", "0.29", "0:1:0.1"),
    ],
    "boolean": [
        ("true", "true", None), ("false", "false", None), ("upper-true", "TRUE", None), ("title-false", "False", None),
        ("one", "1", None), ("zero", "0", None), ("trailing-space", "true ", None), ("zero-length", "", None),
        ("yes", "yes", None), ("format-label-not-payload", "on", "off,on"), ("format-true", "true", "off,on"),
    ],
    "string": [
        ("text", "hello", None), ("nul-is-empty", "\x00", None), ("two-nul", "\x00\x00", None),
        ("space", " ", None), ("unicode", "°C", None), ("astral", "\U0001f600", None),
        ("json-looking", "null", None), ("newline", "a\nb", None), ("format-ignored", "x", "a,b"),
    ],
    "enum": [
        ("in-format", "on", "on,off"), ("second", "off", "on,off"), ("case", "On", "on,off"),
        ("leading-space", " on", "on,off"), ("zero-length", "", "on,off"), ("whole-format", "on,off", "on,off"),
        ("space-significant-match", " b", "a, b"), ("space-significant-miss", "b", "a, b"),
        ("no-format-any", "anything", None), ("no-format-zero-length", "", None), ("empty-format-any", "x", ""),
        ("unicode", "été", "été,hiver"),
    ],
    "color": [
        ("rgb", "rgb,255,255,255", "rgb"), ("rgb-zero", "rgb,0,0,0", "rgb"), ("rgb-float", "rgb,100.5,0.25,1e2", "rgb"),
        ("rgb-leading-dot", "rgb,.5,0,0", "rgb"), ("rgb-trailing-dot", "rgb,5.,0,0", "rgb"),
        ("rgb-neg-zero", "rgb,-0,0,0", "rgb"), ("rgb-above", "rgb,256,0,0", "rgb"), ("rgb-negative", "rgb,-1,0,0", "rgb"),
        ("rgb-1e999", "rgb,1e999,0,0", "rgb"), ("rgb-two", "rgb,1,2", "rgb"), ("rgb-four", "rgb,1,2,3,4", "rgb"),
        ("rgb-trailing-comma", "rgb,1,2,3,", "rgb"), ("rgb-space", "rgb, 1,2,3", "rgb"), ("rgb-nan", "rgb,nan,0,0", "rgb"),
        ("rgb-plus", "rgb,+1,0,0", "rgb"), ("upper-type", "RGB,1,2,3", "rgb"), ("unknown-type", "cmyk,1,2,3,4", None),
        ("zero-length", "", "rgb"), ("type-only", "rgb", "rgb"),
        ("hsv", "hsv,360,100,100", "hsv"), ("hsv-example", "hsv,300,50,75", "hsv"), ("hsv-h-above", "hsv,361,0,0", "hsv"),
        ("hsv-s-above", "hsv,0,101,0", "hsv"), ("hsv-v-above", "hsv,0,0,100.01", "hsv"),
        ("xyz", "xyz,0.25,0.34", "xyz"), ("xyz-ones", "xyz,1,1", "xyz"), ("xyz-above", "xyz,1.1,0", "xyz"),
        ("xyz-three", "xyz,0.5,0.5,0", "xyz"), ("type-not-in-format", "hsv,0,0,0", "rgb"),
        ("second-format", "hsv,10,20,30", "rgb,hsv"), ("no-format-any-type", "xyz,0.1,0.2", None),
    ],
    "datetime": [
        ("calendar", "2024-03-01", None), ("leap-day", "2024-02-29", None), ("not-leap-day", "2023-02-29", None),
        ("century-not-leap", "1900-02-29", None), ("quad-century-leap", "2000-02-29", None),
        ("day-30-feb", "2024-02-30", None), ("month-13", "2024-13-01", None), ("month-00", "2024-00-01", None),
        ("day-00", "2024-03-00", None), ("day-31-apr", "2024-04-31", None), ("year-only", "2024", None),
        ("year-month", "2024-03", None), ("basic-calendar", "20240301", None), ("basic-leap", "20240229", None),
        ("basic-not-leap", "20230229", None), ("ordinal", "2024-060", None), ("ordinal-basic", "2024060", None),
        ("ordinal-366", "2024-366", None), ("ordinal-366-non-leap", "2023-366", None), ("ordinal-000", "2024-000", None),
        ("ordinal-367", "2024-367", None), ("week", "2024-W01", None), ("week-day", "2024-W53-7", None),
        ("week-basic", "2024W537", None), ("week-00", "2024-W00", None), ("week-54", "2024-W54", None),
        ("weekday-8", "2024-W01-8", None), ("weekday-0", "2024-W01-0", None),
        ("hour", "2024-03-01T12", None), ("hour-minute", "2024-03-01T12:30", None),
        ("hms", "2024-03-01T12:30:45", None), ("fraction-dot", "2024-03-01T12:30:45.123", None),
        ("fraction-comma", "2024-03-01T12:30:45,5", None), ("fraction-nanos-z", "2024-03-01T12:30:45.123456789Z", None),
        ("zulu", "2024-03-01T12:30:45Z", None), ("offset", "2024-03-01T12:30:45+05:30", None),
        ("offset-basic", "2024-03-01T12:30:45-0800", None), ("offset-hours", "2024-03-01T12:30:45+05", None),
        ("basic-datetime", "20240301T123045Z", None), ("basic-time", "2024-03-01T123045", None),
        ("ordinal-time", "2024-060T08:00Z", None), ("week-time", "2024-W01-1T08:00", None),
        ("midnight-24", "2024-03-01T24:00", None), ("hour-25", "2024-03-01T25:00", None),
        ("minute-60", "2024-03-01T12:60", None), ("leap-second", "2016-12-31T23:59:60Z", None),
        ("second-61", "2024-03-01T12:30:61", None), ("zone-hour-24", "2024-03-01T12:00+24:00", None),
        ("zone-minute-60", "2024-03-01T12:00+05:60", None), ("zone-one-digit", "2024-03-01T12:00+5", None),
        ("time-needs-day", "2024-03T12:00", None), ("time-on-year", "2024T12", None),
        ("time-on-week-no-day", "2024-W01T12:00", None), ("dangling-t", "2024-03-01T", None),
        ("lowercase-t", "2024-03-01t12:00", None), ("lowercase-z", "2024-03-01T12:00z", None),
        ("space-separator", "2024-03-01 12:00", None), ("short-month", "2024-3-1", None),
        ("two-digit-year", "24-03-01", None), ("dangling-fraction", "2024-03-01T12:00:00.", None),
        ("zero-length", "", None), ("fullwidth-digits", "２０２４", None), ("time-only", "T12:00", None),
        ("midnight-24-hour-only", "2024-03-01T24", None), ("midnight-24-seconds", "2024-03-01T24:00:00", None),
        ("midnight-24-basic", "2024-03-01T2400", None), ("midnight-24-zero-fraction", "2024-03-01T24:00:00.000Z", None),
        ("hour-24-minutes", "2024-01-01T24:30", None), ("hour-24-second", "2024-03-01T24:00:01", None),
        ("hour-24-fraction", "2024-03-01T24:00:00.5", None), ("hour-24-hour-fraction", "2024-03-01T24,5", None),
        ("ordinal-366-quad-century", "2000-366", None), ("ordinal-366-century", "1900-366", None),
        ("week-53-2020", "2020-W53", None), ("week-53-2020-day", "2020-W53-7", None), ("week-53-2026", "2026-W53", None),
        ("week-53-2026-basic", "2026W534", None), ("week-53-2021", "2021-W53", None), ("week-53-2024", "2024-W53", None),
    ],
    "duration": [
        ("zero-seconds", "PT0S", None), ("hours", "PT1H", None), ("minutes", "PT5M", None), ("seconds", "PT1S", None),
        ("example", "PT12H5M46S", None), ("fraction-seconds", "PT1H2M3.5S", None), ("half-second", "PT0.5S", None),
        ("hours-over-24", "PT36H", None), ("leading-zeros", "PT00001H", None), ("bare-pt", "PT", None),
        ("days", "P1D", None), ("pt-days", "PT1D", None), ("date-and-time", "P1DT1H", None),
        ("dangling-dot", "PT1.S", None), ("leading-dot", "PT.5S", None), ("repeat-unit", "PT1H1H", None),
        ("wrong-order", "PT1S1M", None), ("lowercase", "pt1s", None), ("negative", "PT-1S", None),
        ("comma-fraction", "PT1,5S", None), ("fraction-minutes", "PT1.5M", None), ("zero-length", "", None),
        ("no-p", "T1S", None), ("trailing-space", "PT1S ", None),
    ],
    "json": [
        ("empty-array", "[]", None), ("empty-object", "{}", None), ("object", '{"a": 1}', None),
        ("array", "[1, 2, 3]", None), ("nested", '{"a": [true, null, {"b": "c"}], "d": 1.5}', None),
        ("whitespace-around", " [1] ", None), ("scalar-number", "1", None), ("scalar-string", '"s"', None),
        ("scalar-null", "null", None), ("scalar-true", "true", None), ("nan", "NaN", None),
        ("array-nan", "[NaN]", None), ("array-infinity", "[Infinity]", None), ("array-neg-infinity", "[-Infinity]", None),
        ("unquoted-key", "{bad: 1}", None), ("trailing-comma", "[1,]", None), ("zero-length", "", None),
        ("single-quotes", "['a']", None), ("unicode-escape", '["\\u00e9"]', None), ("astral-raw", '["\U0001f600"]', None),
        ("format-ignored", "[1]", '{"type": "array"}'), ("truncated", '{"a": ', None),
        ("float-repr-round-trip", "[912068.5437784989]", None), ("float-long-digits", "[2.283038836859574890682e201]", None),
        ("float-many-digits", "[0.1000000000000000055511151231257827021181583404541015625]", None),
        ("neg-zero-int", "[-0]", None), ("neg-zero-float", "[-0.0]", None),
        ("int-u64-max", "[18446744073709551615]", None), ("int-i64-min", "[-9223372036854775808]", None),
        ("number-overflows-to-infinity", "[1e400]", None), ("lone-surrogate-escape", '["\\ud800"]', None),
    ],
}


# Payload cases where python-sdk's _parse_set_payload deviates from the
# convention (python-sdk#110). Each entry replaces python-sdk's expect and
# source. The generator fails if python-sdk comes to agree with an entry or an
# entry names no case, so the table never goes stale.
_SRC_INT = f"{CONV}#integer (convention.md:87-88): 64-bit signed; {ISSUE} item 2: python-sdk range-checks before step rounding, not after"
_SRC_DT24 = (
    f"{CONV}#datetime (convention.md:132) defers to ISO 8601-1, where hour 24 is only the end of a day "
    f"(minutes, seconds and any fraction zero); {ISSUE} item 3"
)
_SRC_DT366 = f"{CONV}#datetime (convention.md:132) defers to ISO 8601-1: ordinal day 366 only in a leap year; {ISSUE} item 3"
_SRC_DTW53 = (
    f"{CONV}#datetime (convention.md:132) defers to ISO 8601-1: week 53 only in a year with 53 ISO weeks "
    f"(1 January a Thursday, or a leap year whose 1 January is a Wednesday); {ISSUE} item 3"
)
_SRC_JSON = (
    f"{CONV}#json (convention.md:149-150): a number beyond the f64 range decodes to an infinity, which has no "
    f"JSON form, and a lone surrogate is not a Unicode scalar value; {ISSUE} item 4"
)
_DT_RANGE = {"valid": False, "problem": "datetimeRange"}
CONVENTION_OVERRIDES = {
    "integer-int64-max-step-overflow": ({"valid": False, "problem": "integerOutOf64Bit"}, _SRC_INT),
    "datetime-hour-24-minutes": (_DT_RANGE, _SRC_DT24),
    "datetime-hour-24-second": (_DT_RANGE, _SRC_DT24),
    "datetime-hour-24-fraction": (_DT_RANGE, _SRC_DT24),
    "datetime-hour-24-hour-fraction": (_DT_RANGE, _SRC_DT24),
    "datetime-ordinal-366-non-leap": (_DT_RANGE, _SRC_DT366),
    "datetime-ordinal-366-century": (_DT_RANGE, _SRC_DT366),
    "datetime-week-day": (_DT_RANGE, _SRC_DTW53),
    "datetime-week-basic": (_DT_RANGE, _SRC_DTW53),
    "datetime-week-53-2021": (_DT_RANGE, _SRC_DTW53),
    "datetime-week-53-2024": (_DT_RANGE, _SRC_DTW53),
    "json-number-overflows-to-infinity": ({"valid": False, "problem": "jsonInvalid"}, _SRC_JSON),
    "json-lone-surrogate-escape": ({"valid": False, "problem": "jsonInvalid"}, _SRC_JSON),
}


def gen_payloads() -> dict:
    suites = {}
    used = set()
    for dt, rows in PAYLOAD_CASES.items():
        cases = []
        for label, text, fmt in rows:
            case_id = f"{dt}-{label}"
            value, problem = _parse_set_payload(text, dt, fmt, None)
            inp = {"datatype": dt, **payload_field(text)}
            if fmt is not None:
                inp["format"] = fmt
            expect = {"valid": False, "problem": problem} if problem else {"valid": True, "value": value}
            source = f"{CONV}#{dt} (convention.md:74-151, :383-413); {PY} _parse_set_payload"
            if case_id in CONVENTION_OVERRIDES:
                used.add(case_id)
                override, source = CONVENTION_OVERRIDES[case_id]
                if override == expect:
                    raise SystemExit(f"{case_id}: python-sdk now agrees with the convention; remove its override")
                expect = override
            cases.append({"id": case_id, "source": source, "input": inp, "expect": expect})
        suites[dt] = cases
    unused = sorted(set(CONVENTION_OVERRIDES) - used)
    if unused:
        raise SystemExit(f"CONVENTION_OVERRIDES names no case: {unused}")
    return suites


# ── check-value (spec; received values, no step rounding) ────────────────

# Received-value cases beyond PAYLOAD_CASES, where skipping step rounding
# (convention.md:399: rounding is the device's job) changes the verdict or
# where exact comparison with a bound matters.
CHECK_VALUE_CASES = {
    "integer": [
        ("off-step", "5", "0:10:2"),
        ("above-max-step-rounds-in", "11", "1:10:3"), ("below-min-step-rounds-in", "0", "1:10:3"),
        ("float-bound-below", "1", "1.5:10"), ("float-bound-in", "2", "1.5:10"),
        ("exponent-bound-above", "101", "0:1e2"), ("int64-min-open-max", "-9223372036854775808", ":0"),
    ],
    "float": [
        ("off-step", "0.3", "0:10:0.5"), ("above-max-step-rounds-in", "10.2", "0:10:0.5"),
        ("below-min-step-rounds-in", "-0.1", "0:10:0.5"),
        ("just-above-max", "10.000000000000000000000000000001", "0:10"),
        ("just-below-max", "9.999999999999999999999999999999", "0:10"),
        ("same-f64-as-max-above", "10.0000000000000000001", "0:10"),
        ("exponent-bound-in", "1e2", "0:1e2"), ("exponent-bound-above", "100.0000001", "0:1e2"),
        ("neg-zero-at-min", "-0", "0:10"), ("underflow-in-range", "1e-400", "0:10"),
        ("max-f64-below-open-min", "-1.7976931348623157e308", "0:"),
    ],
}

# The PAYLOAD_CASES overrides that still apply without step rounding.
CHECK_VALUE_OVERRIDES = [k for k in CONVENTION_OVERRIDES if not k.startswith("integer-")]

_NUMERIC = ("integer", "float")


def _strip_step(dt: str, fmt):
    """The numeric format without its step, so python-sdk does not round."""
    if dt in _NUMERIC and isinstance(fmt, str) and fmt.count(":") == 2:
        return fmt.rsplit(":", 1)[0]
    return fmt


def _verdict(text: str, dt: str, fmt) -> dict:
    _, problem = _parse_set_payload(text, dt, fmt, None)
    return {"valid": False, "problem": problem} if problem else {"valid": True}


def gen_check_value() -> list:
    """Received values and $target payloads, checked as datatype::check_value does.

    The wire payload is the case text, with "" sent as 0x00 (a zero-length
    payload deletes the topic and is not a value). The expect value is python-sdk
    _parse_set_payload on the decoded text with any step removed from the
    format, except for CHECK_VALUE_OVERRIDES.
    """
    cases = []
    used = set()
    disagreements = []
    for dt in PAYLOAD_CASES:
        for label, text, fmt in PAYLOAD_CASES[dt] + CHECK_VALUE_CASES.get(dt, []):
            payload_id = f"{dt}-{label}"
            wire = "\x00" if text == "" else text
            decoded = "" if wire == "\x00" else wire
            if text == "":
                label = label.replace("zero-length", "nul")
            case_id = f"{dt}-{label}"
            inp = {"datatype": dt, **payload_field(wire)}
            if fmt is not None:
                inp["format"] = fmt
            expect = _verdict(decoded, dt, _strip_step(dt, fmt))
            source = f"{CONV}#{dt} (convention.md:74-151, :383-399); {PY} _parse_set_payload with the step removed"
            if payload_id in CHECK_VALUE_OVERRIDES:
                used.add(payload_id)
                override, source = CONVENTION_OVERRIDES[payload_id]
                if override == expect:
                    raise SystemExit(f"check-value {case_id}: python-sdk now agrees with the convention; remove its override")
                expect = override
            rounded = _verdict(decoded, dt, fmt)
            if fmt != _strip_step(dt, fmt) and rounded != expect:
                source = (
                    f"{CONV}#payload (convention.md:399): step rounding is the device's job, so a received value is "
                    f"not rounded; {PY} _parse_set_payload rounds first and gives {json.dumps(rounded)}"
                )
                disagreements.append(case_id)
            cases.append({"id": case_id, "source": source, "input": inp, "expect": expect})
    unused = sorted(set(CHECK_VALUE_OVERRIDES) - used)
    if unused:
        raise SystemExit(f"CHECK_VALUE_OVERRIDES names no case: {unused}")
    if not disagreements:
        raise SystemExit("check-value: no case shows the step-rounding difference")
    return cases


# ── description (spec, from convention.md) ─────────────────────────────────


def gen_description() -> list:
    src_dev = f"{CONV}#device-attributes (convention.md:210-222), derived from the convention: python-sdk has no $description parser"
    src_node = f"{CONV}#node-attributes (convention.md:300-306), derived from the convention"
    src_prop = f"{CONV}#property-attributes (convention.md:336-345), derived from the convention"
    src_fmt = f"{CONV}#formats (convention.md:383-395), derived from the convention"
    src_fwd = f"{CONV}#forward-compatibility (convention.md:659-668), derived from the convention"

    def doc(**kw):
        d = {"homie": "5.0", "version": 1}
        d.update(kw)
        return {k: v for k, v in d.items() if v is not _DROP}

    def prop(**kw):
        p = {"datatype": "integer"}
        p.update(kw)
        return {k: v for k, v in p.items() if v is not _DROP}

    def with_prop(p, node_id="node", prop_id="prop"):
        return doc(nodes={node_id: {"properties": {prop_id: p}}})

    full = {
        "homie": "5.0",
        "version": 7,
        "name": "Supercar",
        "type": "org.example.car",
        "nodes": {
            "engine": {
                "name": "Car engine",
                "type": "org.example.engine",
                "properties": {
                    "temperature": {"name": "Engine temperature", "unit": "°C", "datatype": "float", "format": "-20:120"},
                    "on": {"datatype": "boolean", "settable": True, "retained": True},
                    "mode": {"datatype": "enum", "format": "eco,sport"},
                    "color": {"datatype": "color", "format": "rgb,hsv"},
                    "event": {"datatype": "string", "retained": False},
                    "config": {"datatype": "json", "format": '{"type": "object"}'},
                },
            }
        },
        "children": ["wheel-1", "wheel-2"],
        "extensions": ["org.example.feature"],
    }
    rows = [
        ("minimal", doc(), True, None, src_dev),
        ("full", full, True, None, src_dev),
        ("future-minor", doc(homie="5.3"), True, None, src_dev),
        ("minor-two-digits", doc(homie="5.10"), True, None, src_dev),
        ("version-zero", doc(version=0), True, None, src_dev),
        ("version-2-53-minus-1", doc(version=2**53 - 1), True, None, src_dev),
        ("version-u64-max", doc(version=2**64 - 1), True, None, src_dev),
        ("version-negative", doc(version=-5), True, None, src_dev),
        ("version-i64-min", doc(version=-(2**63)), True, None, src_dev),
        ("formats-legal", doc(nodes={"n": {"properties": {
            "a": prop(format=":"), "b": prop(format="-5:35:2.5"), "c": prop(datatype="float", format="10.123:15.123"),
            "d": prop(datatype="float", format="0::1e-400"), "e": prop(datatype="boolean", format="off,on"),
            "f": prop(datatype="enum", format=" a,a "), "g": prop(datatype="color", format="xyz,rgb,hsv"),
            "h": prop(datatype="string", format="anything")}}}), True, None, src_fmt),
        ("empty-nodes", doc(nodes={}), True, None, src_dev),
        ("node-no-properties", doc(nodes={"node": {}}), True, None, src_node),
        ("node-empty-properties", doc(nodes={"node": {"properties": {}}}), True, None, src_node),
        ("child-root-only", doc(root="bridge"), True, None, src_dev),
        ("child-root-and-parent", doc(root="bridge", parent="dualrelay"), True, None, src_dev),
        ("children", doc(children=["light1", "light2"]), True, None, src_dev),
        ("unknown-top-level-field", doc(**{"imported-from": {"source": "x"}}), True, None, src_fwd),
        ("unknown-node-field", doc(nodes={"node": {"icon": "car", "properties": {}}}), True, None, src_fwd),
        ("unknown-property-field", with_prop(prop(precision=2)), True, None, src_fwd),
        ("every-datatype", doc(nodes={"n": {"properties": {
            "a": {"datatype": "integer"}, "b": {"datatype": "float"}, "c": {"datatype": "boolean"},
            "d": {"datatype": "string"}, "e": {"datatype": "enum", "format": "x"}, "f": {"datatype": "color", "format": "xyz"},
            "g": {"datatype": "datetime"}, "h": {"datatype": "duration"}, "i": {"datatype": "json"}}}}), True, None, src_prop),
        ("ids-digits-hyphens", doc(nodes={"0-a": {"properties": {"-1-": prop()}}}), True, None, src_node),
        ("settable-false", with_prop(prop(settable=False)), True, None, src_prop),
        ("unit-unicode", with_prop(prop(unit="m³")), True, None, src_prop),
        # invalid
        ("not-json", "{", False, "notJson", src_dev),
        ("top-level-array", [], False, "notObject", src_dev),
        ("top-level-string", "\"x\"", False, "notObject", src_dev),
        ("empty-object", {}, False, "homieMissing", src_dev),
        ("homie-missing", doc(homie=_DROP), False, "homieMissing", src_dev),
        ("homie-major-only", doc(homie="5"), False, "homieVersion", src_dev),
        ("homie-with-patch", doc(homie="5.0.0"), False, "homieVersion", src_dev),
        ("homie-major-4", doc(homie="4.0"), False, "homieVersion", src_dev),
        ("homie-major-6", doc(homie="6.0"), False, "homieVersion", src_dev),
        ("homie-number", doc(homie=5.0), False, "homieVersion", src_dev),
        ("homie-letter-minor", doc(homie="5.x"), False, "homieVersion", src_dev),
        ("homie-empty-minor", doc(homie="5."), False, "homieVersion", src_dev),
        ("homie-null", doc(homie=None), False, "homieVersion", src_dev),
        ("version-missing", doc(version=_DROP), False, "versionMissing", src_dev),
        ("version-string", doc(version="1"), False, "versionNotInteger", src_dev),
        ("version-fraction", doc(version=1.5), False, "versionNotInteger", src_dev),
        ("version-bool", doc(version=True), False, "versionNotInteger", src_dev),
        ("version-null", doc(version=None), False, "versionNotInteger", src_dev),
        ("name-null", doc(name=None), False, "nameType", src_dev),
        ("name-number", doc(name=1), False, "nameType", src_dev),
        ("type-null", doc(type=None), False, "typeType", src_dev),
        ("nodes-array", doc(nodes=[]), False, "nodesNotObject", src_dev),
        ("nodes-null", doc(nodes=None), False, "nodesNotObject", src_dev),
        ("children-not-array", doc(children="a"), False, "childrenNotArray", src_dev),
        ("children-invalid-id", doc(children=["Light1"]), False, "invalidChildId", src_dev),
        ("children-non-string", doc(children=[1]), False, "invalidChildId", src_dev),
        ("root-invalid-id", doc(root="Bridge"), False, "invalidRootId", src_dev),
        ("root-null", doc(root=None), False, "invalidRootId", src_dev),
        ("parent-without-root", doc(parent="dualrelay"), False, "parentWithoutRoot", src_dev),
        ("parent-invalid-id", doc(root="bridge", parent="dual_relay"), False, "invalidParentId", src_dev),
        ("extensions-not-array", doc(extensions="org.example.x"), False, "extensionsNotArray", src_dev),
        ("extensions-non-string", doc(extensions=[1]), False, "extensionsNotArray", src_dev),
    ]
    # Valid documents with nodes or properties left out (convention.md:664-668):
    # (label, document, [(node, property or None, hint)], source).
    ignored_rows = [
        ("node-id-upper", doc(nodes={"Node": {}}), [("Node", None, "invalidNodeId")], src_node),
        ("node-id-underscore", doc(nodes={"my_node": {}}), [("my_node", None, "invalidNodeId")], src_node),
        ("node-id-empty", doc(nodes={"": {}}), [("", None, "invalidNodeId")], src_node),
        ("node-id-dollar", doc(nodes={"$node": {}}), [("$node", None, "invalidNodeId")], src_node),
        ("node-not-object", doc(nodes={"node": []}), [("node", None, "nodeNotObject")], src_node),
        ("node-name-null", doc(nodes={"node": {"name": None}}), [("node", None, "nameType")], src_node),
        ("node-type-number", doc(nodes={"node": {"type": 3}}), [("node", None, "typeType")], src_node),
        ("properties-array", doc(nodes={"node": {"properties": []}}), [("node", None, "propertiesNotObject")], src_node),
        ("property-id-upper", with_prop(prop(), prop_id="Prop"), [("node", "Prop", "invalidPropertyId")], src_prop),
        ("property-id-dot", with_prop(prop(), prop_id="a.b"), [("node", "a.b", "invalidPropertyId")], src_prop),
        ("property-not-object", with_prop("integer"), [("node", "prop", "propertyNotObject")], src_prop),
        ("datatype-missing", with_prop(prop(datatype=_DROP)), [("node", "prop", "datatypeMissing")], src_prop),
        ("datatype-unknown", with_prop(prop(datatype="number")), [("node", "prop", "datatypeUnknown")], src_prop),
        ("datatype-upper", with_prop(prop(datatype="Integer")), [("node", "prop", "datatypeUnknown")], src_prop),
        ("datatype-null", with_prop(prop(datatype=None)), [("node", "prop", "datatypeUnknown")], src_prop),
        ("settable-string", with_prop(prop(settable="true")), [("node", "prop", "settableType")], src_prop),
        ("retained-number", with_prop(prop(retained=1)), [("node", "prop", "retainedType")], src_prop),
        ("retained-null", with_prop(prop(retained=None)), [("node", "prop", "retainedType")], src_prop),
        ("format-nested-object", with_prop(prop(datatype="json", format={"type": "object"})), [("node", "prop", "formatType")], src_fmt),
        ("unit-number", with_prop(prop(unit=5)), [("node", "prop", "unitType")], src_prop),
        ("enum-format-missing", with_prop(prop(datatype="enum")), [("node", "prop", "formatMissing")], src_fmt),
        ("color-format-missing", with_prop(prop(datatype="color")), [("node", "prop", "formatMissing")], src_fmt),
        ("enum-format-empty", with_prop(prop(datatype="enum", format="")), [("node", "prop", "formatMissing")], src_fmt),
        ("color-format-empty", with_prop(prop(datatype="color", format="")), [("node", "prop", "formatMissing")], src_fmt),
        ("enum-format-empty-value", with_prop(prop(datatype="enum", format="a,,a")), [("node", "prop", "formatIllegal")], src_fmt),
        ("enum-format-duplicate", with_prop(prop(datatype="enum", format="a,b,a")), [("node", "prop", "formatIllegal")], src_fmt),
        ("color-format-unknown-type", with_prop(prop(datatype="color", format="rgb,cmyk")), [("node", "prop", "formatIllegal")], src_fmt),
        ("boolean-format-one-entry", with_prop(prop(datatype="boolean", format="on")), [("node", "prop", "formatIllegal")], src_fmt),
        ("boolean-format-three-entries", with_prop(prop(datatype="boolean", format="a,b,c")), [("node", "prop", "formatIllegal")], src_fmt),
        ("integer-format-grammar", with_prop(prop(format="abc:1:-3")), [("node", "prop", "formatIllegal")], src_fmt),
        ("integer-format-one-part", with_prop(prop(format="5")), [("node", "prop", "formatIllegal")], src_fmt),
        ("integer-format-float-bound", with_prop(prop(format="1.5:2")), [("node", "prop", "formatIllegal")], src_fmt),
        ("integer-format-zero-step", with_prop(prop(format="0:10:0")), [("node", "prop", "formatIllegal")], src_fmt),
        ("float-format-grammar", with_prop(prop(datatype="float", format="x:y")), [("node", "prop", "formatIllegal")], src_fmt),
        ("future-datatype-keeps-rest", doc(nodes={
            "n": {"properties": {"ok": prop(), "p": prop(datatype="vector")}},
            "m": {"properties": {"q": prop(datatype="string")}}}), [("n", "p", "datatypeUnknown")], src_fwd),
        ("bad-node-keeps-rest", doc(nodes={"n": {"properties": {"ok": prop()}}, "Bad": {}}), [("Bad", None, "invalidNodeId")], src_fwd),
    ]
    cases = []
    for label, document, valid, hint, src in rows:
        text = document if isinstance(document, str) else json.dumps(document, ensure_ascii=False)
        expect = {"valid": valid}
        if not valid:
            expect["error"] = hint
        cases.append({"id": f"description-{label}", "source": src, "input": {"payload": text}, "expect": expect})
    for label, document, ignored, src in ignored_rows:
        text = json.dumps(document, ensure_ascii=False)
        expect = {
            "valid": True,
            "ignored": [{"node": n, "property": p, "error": hint} for n, p, hint in ignored],
        }
        cases.append({"id": f"description-{label}", "source": src, "input": {"payload": text}, "expect": expect})
    return cases


_DROP = object()


# ── python-sdk: float-format ───────────────────────────────────────────────────


def f64_bits(x: float) -> str:
    return struct.pack(">d", x).hex()


def bits_f64(h: str) -> float:
    return struct.unpack(">d", bytes.fromhex(h))[0]


def gen_float_format() -> list:
    values = []
    fixed = [
        0.0, -0.0, 1.0, -1.0, 0.1, 0.2, 0.3, 0.1 + 0.2, 1 / 3, 2 / 3, -1 / 3, 0.5, 1.5, 2.5, 100.0, 1234.5,
        123456789.0, 1e-4, 1.0001e-4, 9.999999999999999e-5, 1e-5, 1.5e-5, -1e-5, 0.00012345, 1e15, 9999999999999998.0,
        1e16, 1.0000000000000002e16, 1.5e16, -1e16, 12345678901234567.0, 2.0**53, 2.0**53 + 2, 2.0**63, 2.0**64,
        1e21, 1e22, 1e23, 1e100, 1e300, 1e308, 1.7976931348623157e308, -1.7976931348623157e308, 2.2250738585072014e-308,
        2.225073858507201e-308, 5e-324, -5e-324, 1e-320, 1e-300, 1e-100, 4.35e-6, 3.14159, 2.718281828459045,
        6.02214076e23, 6.62607015e-34, 299792458.0, 0.1 * 3, 1.1, 2.675, 1e-7, 123e-20, 9007199254740993.0, 0.30000000000000004,
        float(2**31), float(-2**31), 65535.0, 4294967295.0, 1e10, 1e-10, 12.5, -0.001, 0.0001, 99.99, 1e14 + 0.5, 1e15 + 0.3,
        float("inf"), float("-inf"), float("nan"),
    ]
    values.extend(fixed)
    # Exponent sweep: powers of ten and a nudge either side.
    for e in range(-330, 310, 7):
        try:
            v = float(f"1e{e}")
        except OverflowError:
            continue
        values.append(v)
        if v != 0.0 and math.isfinite(v):
            values.append(math.nextafter(v, math.inf))
            values.append(math.nextafter(v, -math.inf))
    # Integers as floats.
    for n in (2, 7, 10, 42, 1000, 10**6, 10**15 - 1, 10**15, 10**16 - 2, 10**17):
        values.append(float(n))
        values.append(-float(n))
    rng = random.Random(20261010)
    for _ in range(60):
        values.append(rng.uniform(-1e6, 1e6))
    for _ in range(40):
        values.append(bits_f64(f"{rng.getrandbits(64):016x}"))
    for _ in range(20):
        values.append(rng.random() * 10 ** rng.randint(-20, 20))
    cases, seen = [], set()
    for v in values:
        bits = f64_bits(v)
        if bits in seen:
            continue
        seen.add(bits)
        cases.append(
            {
                "id": f"float-format-{bits}",
                "source": f"{PY} _format_float (repr(float(x)).replace('e+', 'e'))",
                "input": {"f64_bits": bits, "repr_hint": repr(v)},
                "expect": {"payload": _format_float(v)},
            }
        )
    return cases


# ── python-sdk: float32-format ─────────────────────────────────────────────────


def f32_of_bits(h: str) -> np.float32:
    return np.frombuffer(bytes.fromhex(h), dtype=">f4")[0].astype(np.float32)


def format_float32(v: np.float32):
    """numpy's shortest round-trip f32 digits in python-sdk's _format_float layout.

    The digits (at most 9 significant) parse to an f64 whose repr keeps them, so
    _format_float lays them out as python-sdk lays out a float.
    """
    if not np.isfinite(v):
        return None
    shortest = np.format_float_scientific(v, unique=True)
    assert np.float32(float(shortest)) == v
    return _format_float(float(shortest))


def gen_float32_format() -> list:
    f32_max = float(np.finfo(np.float32).max)
    tiny = float(np.finfo(np.float32).tiny)
    values = [
        0.0, -0.0, 1.0, -1.0, 21.4, -21.4, 40.0, 0.1, 0.2, 0.3, 1 / 3, 0.5, 2.5, 100.0, 1234.5, 123456789.0,
        16777216.0, 16777217.0, 2.0**31, 2.0**63, 1e-4, 9.999e-5, 1e-5, 1.5e-5, 0.00012345, 1e15, 1e16, 1.5e16,
        1e-38, 1e38, f32_max, -f32_max, tiny, -tiny, 1e-45, 3e-45, 1e-40, 2097152.25, 2097153.75, 3.14159,
        2.718281828459045, 6.02214076e23, 299792458.0, 65535.0, 4294967295.0, 99.99, 0.001, -0.001, 12.5,
        float("inf"), float("-inf"), float("nan"),
    ]
    vals = [np.float32(v) for v in values]
    # Subnormal edges and the largest subnormal.
    for bits in (0x00000001, 0x00000002, 0x00000003, 0x007FFFFF, 0x00400000, 0x80000001, 0x7F7FFFFF, 0x7F7FFFFE):
        vals.append(f32_of_bits(f"{bits:08x}"))
    # Exponent sweep: powers of ten and a nudge either side.
    for e in range(-45, 39):
        v = np.float32(float(f"1e{e}"))
        vals.append(v)
        if v != 0 and np.isfinite(v):
            vals.append(np.nextafter(v, np.float32(np.inf)))
            vals.append(np.nextafter(v, np.float32(-np.inf)))
    rng = random.Random(20261011)
    for _ in range(60):
        vals.append(np.float32(rng.uniform(-1e6, 1e6)))
    for _ in range(60):
        vals.append(f32_of_bits(f"{rng.getrandbits(32):08x}"))
    for _ in range(20):
        vals.append(np.float32(rng.random() * 10 ** rng.randint(-20, 20)))
    cases, seen = [], set()
    for v in vals:
        bits = struct.pack(">f", float(v)).hex()
        if bits in seen:
            continue
        seen.add(bits)
        cases.append(
            {
                "id": f"float32-format-{bits}",
                "source": f"numpy {np.__version__} shortest float32 digits, laid out by {PY} _format_float",
                "input": {"f32_bits": bits, "repr_hint": str(v)},
                "expect": {"payload": format_float32(v)},
            }
        )
    return cases


# ── python-sdk: sanitize-id ────────────────────────────────────────────────────


def gen_sanitize_id() -> list:
    corpus = [
        ("empty", ""), ("already-valid", "abc-123"), ("upper", "ABC"), ("mixed", "MyDevice_01"),
        ("underscores", "a__b___c"), ("dots", "a.b..c"), ("mixed-separators", "a_. _b"), ("space", "a b"),
        ("tab", "a\tb"), ("newline", "a\nb"), ("cr", "a\rb"), ("vt", "a\x0bb"), ("ff", "a\x0cb"),
        ("file-sep-1c", "a\x1cb"), ("group-sep-1d", "a\x1db"), ("record-sep-1e", "a\x1eb"), ("unit-sep-1f", "a\x1fb"),
        ("nel-85", "a\x85b"), ("nbsp-a0", "a\xa0b"), ("ogham-space", "a b"), ("en-quad", "a b"),
        ("hair-space", "a b"), ("zwsp-not-space", "a​b"), ("line-sep", "a b"), ("para-sep", "a b"),
        ("narrow-nbsp", "a b"), ("math-space", "a b"), ("ideographic-space", "a　b"), ("bom", "a﻿b"),
        ("leading-trailing", "  -abc-  "), ("only-separators", "_ . _"), ("hyphen-runs", "a---b"),
        ("kelvin-sign", "K"), ("kelvin-in-word", "300Kelvin"), ("angstrom-sign", "Å"),
        ("ohm-sign", "Ω"), ("turkish-dotted-i", "İstanbul"), ("turkish-dotless-i", "ızmir"),
        ("german-sharp-s", "Straße"), ("capital-sharp-s", "STRAẞE"), ("e-acute", "Café"),
        ("e-combining", "Café"), ("greek-sigma", "Σσς"), ("cyrillic", "Москва"),
        ("fullwidth-latin", "Ａｂｃ"), ("fullwidth-digits", "１２"), ("superscript-two", "m²"),
        ("roman-numeral", "ⅠⅡ"), ("ligature-fi", "ﬁle"), ("long-s", "ſ"), ("emoji", "a\U0001f600b"),
        ("astral-letter", "\U0001d400"), ("degree", "20°C"), ("serial", "SN: 0000-1002_4176"),
        ("composed", "panel-1/bess.2"), ("slash", "a/b"), ("dollar", "$state"), ("plus-hash", "a+b#c"),
        ("unicode-digit", "٣٤"), ("dz-digraph", "Ǆ"), ("title-dz", "ǅ"), ("armenian-ligature", "և"),
        ("i-with-dot-only", "İ"), ("upper-a-ring", "Å"), ("deseret", "\U00010400"),
    ]
    cases = []
    for label, value in corpus:
        cases.append(
            {
                "id": f"sanitize-{label}",
                "source": f"{PY} sanitize_homie_id",
                "input": {"value": value},
                "expect": {"id": sanitize_homie_id(value)},
            }
        )
    return cases


# ── python-sdk: set-payload ────────────────────────────────────────────────────


def gen_set_payload() -> list:
    rows = [
        # (label, datatype, payload, format, current)
        ("int-no-format", "integer", "5", None, None),
        ("int-min-base", "integer", "5", "0:10:2", None),
        ("int-max-base", "integer", "5", ":10:2", None),
        ("int-max-base-ignores-current", "integer", "5", ":10:2", 1),
        ("int-current-base", "integer", "5", "::2", 1),
        ("int-current-base-even", "integer", "5", "::2", 0),
        ("int-current-null-no-round", "integer", "5", "::2", None),
        ("int-current-float-base", "integer", "5", "::2", 0.5),
        ("int-current-float-base-whole", "integer", "6", "::3", 1.5),
        ("int-negative-step-ignored", "integer", "5", "0:10:-2", None),
        ("int-zero-step-ignored", "integer", "5", "0:10:0", None),
        ("int-step-not-dividing-top", "integer", "10", "0:10:3", None),
        ("int-step-not-dividing-mid", "integer", "8", "0:10:3", None),
        ("int-step-fraction-not-whole", "integer", "3", "0:10:2.5", None),
        ("int-step-fraction-whole", "integer", "4", "0:10:2.5", None),
        ("int-step-decimal-base", "integer", "3", "0.5:10:1", None),
        ("int-below-min-after-round", "integer", "-3", "0:10:2", None),
        ("int-rounds-into-range", "integer", "-1", "0:10:2", None),
        ("int-above-max-after-round", "integer", "11", "0:10:2", None),
        ("int-negative-values", "integer", "-7", "-10:-2:4", None),
        ("int-negative-tie", "integer", "-8", "-10:10:4", None),
        ("int-leading-zero-unchanged", "integer", "004", "0:10:2", None),
        ("int-leading-zero-changed", "integer", "005", "0:10:2", None),
        ("int-neg-zero-unchanged", "integer", "-0", "0:10:2", None),
        ("int-max", "integer", "9223372036854775807", "::1", 0),
        ("int-format-float-bounds", "integer", "5", "0.5:5.5", None),
        ("int-format-exponent-bounds", "integer", "500", "1e2:1e3:1e2", None),
        ("int-huge-step", "integer", "5", "0::1e30", None),
        ("float-no-format", "float", "1.25", None, None),
        ("float-step-half", "float", "0.3", "0:10:0.5", None),
        ("float-step-tie-up", "float", "0.25", "0:10:0.5", None),
        ("float-step-negative-tie-up", "float", "-0.25", "-10:10:0.5", None),
        ("float-step-tenth-reformat", "float", "0.29", "0:1:0.1", None),
        ("float-step-tenth-0-7", "float", "0.66", "0:1:0.1", None),
        ("float-step-third", "float", "0.5", "0:1:0.333", None),
        ("float-step-exact-unchanged", "float", "1.5", "0:10:0.5", None),
        ("float-step-unchanged-text-kept", "float", "1.50", "0:10:0.5", None),
        ("float-step-exponent-text-kept", "float", "15e-1", "0:10:0.5", None),
        ("float-step-changes-exponent-text", "float", "16e-1", "0:10:0.5", None),
        ("float-max-base", "float", "0.3", ":1:0.25", None),
        ("float-current-base", "float", "0.3", "::0.25", 0.1),
        ("float-current-base-int", "float", "2.6", "::1", 1),
        ("float-current-binary-fraction", "float", "1", "::0.5", 0.2),
        ("float-current-third", "float", "1", "::1", 1 / 3),
        ("float-current-large", "float", "1e20", "::1", 0.1),
        ("float-step-big-exponent", "float", "1.5e300", "::1e300", 0),
        ("float-step-tiny", "float", "3e-300", "0::2e-300", None),
        ("float-underflow-payload-rounds", "float", "1e-400", "::1", 0),
        ("float-underflow-payload-kept", "float", "1e-400", None, None),
        ("float-below-min", "float", "-0.1", "0:10", None),
        ("float-above-max", "float", "10.0001", "0:10", None),
        ("float-below-min-after-round", "float", "-0.3", "0:10:0.5", None),
        ("float-in-range-after-round", "float", "-0.2", "0:10:0.5", None),
        ("float-above-max-after-round", "float", "10.3", "0:10.2:0.5", None),
        ("float-bounds-exponent", "float", "5", "1e-1:1e1", None),
        ("float-bounds-huge", "float", "1e308", "0:1e400", None),
        ("float-neg-zero-unchanged", "float", "-0.0", "0:10:1", None),
        ("float-step-result-zero", "float", "0.2", "-1:1:1", None),
        ("float-step-result-neg-zero-base", "float", "-0.2", "::1", -0.0),
        ("float-format-unparseable", "float", "5", "x:y:z", None),
        ("float-format-open", "float", "5", ":", None),
        ("float-format-one-part", "float", "5", "5", None),
        ("float-grammar", "float", "1e+5", None, None),
        ("float-1e999", "float", "1e999", None, None),
        ("float-long-no-format", "float", "1." + "7" * 4000, None, None),
        ("float-long-step", "float", "1." + "7" * 4000, "0:1e400:0.001", None),
        ("float-long-above-max", "float", "1." + "0" * 4000 + "1", "0:1", None),
        ("float-long-equal-max", "float", "1." + "0" * 4000, "0:1", None),
        ("float-long-below-min", "float", "-0." + "0" * 4000 + "1", "0:1", None),
        ("float-long-above-tie", "float", "0.25" + "0" * 4000 + "1", "0:10:0.5", None),
        ("float-long-below-tie", "float", "0.24" + "9" * 4000, "0:10:0.5", None),
        ("float-long-negative-below-tie", "float", "-0.25" + "0" * 4000 + "1", "-10:10:0.5", None),
        ("float-long-current-base", "float", "2.5" + "0" * 4000 + "1", "::1", 0.5),
        ("int-grammar", "integer", "1.0", None, None),
        ("int-out-of-range", "integer", "9223372036854775808", None, None),
        ("boolean", "boolean", "true", None, None),
        ("enum", "enum", "b", "a,b", None),
        ("enum-miss", "enum", "c", "a,b", None),
        ("color", "color", "rgb,1,2,3", "rgb", None),
        ("datetime", "datetime", "2024-02-29", None, None),
        ("duration", "duration", "PT1H", None, None),
        ("string-nul", "string", "\x00", None, None),
        ("string-text", "string", "abc", None, 5),
        ("json-object", "json", '{"b": 1, "a": [1.5, "x"]}', None, None),
        ("json-scalar", "json", "1", None, None),
        ("unknown-datatype-as-string", "number", "\x00", None, None),
    ]
    cases = []
    for label, dt, text, fmt, current in rows:
        try:
            expect = set_result(text, dt, fmt, current)
        except Exception as e:  # an oracle crash is not a vector
            print(f"set-payload {label}: python raised {e!r}; skipped", file=sys.stderr)
            continue
        inp = {"datatype": dt, **payload_field(text), "format": fmt, "current": current}
        if isinstance(current, float):
            inp["current_f64_bits"] = f64_bits(current)
        cases.append({"id": f"set-{label}", "source": f"{PY} _parse_set_payload", "input": inp, "expect": expect})
    return cases


# ── python-sdk: json-dumps ─────────────────────────────────────────────────────


def gen_json_dumps() -> list:
    values = [
        ("empty-object", {}), ("empty-array", []), ("null-in-array", [None]), ("bools", [True, False]),
        ("ints", [0, 1, -1, 2**63 - 1, -(2**63), 2**64 - 1]), ("floats", [0.0, -0.0, 1.0, 0.1, 1e16, 1e-5, 1.5e300, 5e-324, 100.0, 1e22]),
        ("float-third", [1 / 3]), ("sorted-keys", {"b": 1, "a": 2, "c": {"z": 1, "y": 2}}),
        ("key-code-point-order", {"é": 1, "z": 2, "Z": 3, "\U0001f600": 4, "￿": 5, "": 6}),
        ("nested", {"a": [{"b": [[], {}]}, [1, [2, [3]]]]}),
        ("ascii-string", ["hello world"]), ("quote-backslash", ['a"b\\c']), ("slash", ["a/b</script>"]),
        ("controls", ["\x00\x01\x07\x08\t\n\x0b\x0c\r\x1b\x1f"]), ("del", ["\x7f"]), ("latin1", ["café °C ÿ"]),
        ("bmp", ["€  ﻿￿"]), ("astral", ["\U0001f600 \U00010000 \U0010ffff"]),
        ("cjk", ["中文"]), ("unicode-keys", {"°C": "µs"}), ("empty-string", [""]),
        ("deep", [[[[[[[[["x"]]]]]]]]]), ("mixed", {"k": [1, 1.5, "s", None, True, {"n": []}]}),
        ("description-like", {"homie": "5.0", "name": "Café", "nodes": {"n": {"properties": {"p": {"datatype": "float", "unit": "°C"}}}}}),
        ("float-big-int", [1e15, 1e16, 12345678901234567890.0]), ("float-small", [1e-4, 9.9e-5, 0.0001234]),
    ]
    cases = []
    for label, v in values:
        cases.append(
            {
                "id": f"json-dumps-{label}",
                "source": f"{PY} json.dumps(value, sort_keys=True) as used by Device._description_content_hash",
                "input": {"json": json.dumps(v, ensure_ascii=False)},
                "expect": {"text": json.dumps(v, sort_keys=True)},
            }
        )
    # Inputs written with escapes, whitespace and exponents: the parsed value is what counts.
    raw = [
        ("escaped-input", '["\\u00e9\\ud83d\\ude00\\/\\n"]'),
        ("whitespace-input", ' { "b" : [ 1 , 2 ] ,\n "a" : null } '),
        ("exponent-input", "[1e2, 1E-7, 2.50, 0.10]"),
        ("int-vs-float-input", "[1, 1.0, -1, -1.0]"),
        ("neg-zero-input", "[-0, -0.0, 0e5, -0e-5]"),
        ("float-round-trip-input", "[912068.5437784989, 2.283038836859574890682e201, 1.7976931348623157e308, 4.9e-324]"),
    ]
    for label, text in raw:
        cases.append(
            {
                "id": f"json-dumps-{label}",
                "source": f"{PY} json.dumps(json.loads(text), sort_keys=True)",
                "input": {"json": text},
                "expect": {"text": json.dumps(json.loads(text), sort_keys=True)},
            }
        )
    return cases


# ── python-sdk: encode-duration ────────────────────────────────────────────────


def gen_encode_duration() -> list:
    rows = [
        (0, 0), (0, 1), (0, 500000), (0, 999999), (1, 0), (59, 0), (60, 0), (61, 0), (3599, 0), (3600, 0), (3601, 0),
        (3661, 0), (3661, 500000), (3600, 1), (3720, 0), (86399, 999999), (86400, 0), (90061, 0), (90061, 120000),
        (12 * 3600 + 5 * 60 + 46, 0), (10**9, 0), (999999999 * 86400 + 86399, 999999), (0, 100000), (0, 10), (7, 250000),
    ]
    cases = []
    for secs, micros in rows:
        payload, problem = _encode_duration(datetime.timedelta(seconds=secs, microseconds=micros))
        assert problem is None
        cases.append(
            {
                "id": f"encode-duration-{secs}s-{micros}us",
                "source": f"{PY} _encode_duration(timedelta)",
                "input": {"total_seconds": secs, "microseconds": micros},
                "expect": {"payload": payload},
            }
        )
    return cases


# ── python-sdk: description-wire ───────────────────────────────────────────────
#
# Each tree is a list of device specs in creation order; a child names its
# parent by id. Node and property specs are dicts of constructor arguments.


def P(id, datatype, **kw):
    return {"id": id, "datatype": datatype, **kw}


def N(id, *props, name=None, type=None):
    return {"id": id, "name": name, "type": type, "properties": list(props)}


def D(id, *nodes, name=None, type=None, parent=None, extensions=None, extras=None):
    return {"id": id, "name": name, "type": type, "parent": parent, "nodes": list(nodes),
            "extensions": extensions, "extras": extras}


SCHEMA_FMT = {"type": "object", "properties": {"level": {"type": "integer", "minimum": 0, "maximum": 100}}, "required": ["level"]}

TREES = [
    ("bare-no-type", [D("dev")]),
    ("bare-typed", [D("dev", type="energy.ebus.device.panel")]),
    ("named", [D("dev", name="Living Room", type="t")]),
    ("empty-name-defaults-to-id", [D("dev", name="", type="t")]),
    ("empty-type-kept", [D("dev", type="")]),
    ("unicode-name", [D("dev", name="Café ☕ \U0001f600 中文", type="té")]),
    ("node-untyped", [D("dev", N("n"))]),
    ("node-typed-named", [D("dev", N("n", name="Nöde", type="org.example.node"))]),
    ("node-empty-type-kept", [D("dev", N("n", type=""))]),
    ("node-empty-name-defaults", [D("dev", N("n", name=""))]),
    ("prop-minimal", [D("dev", N("n", P("p", "integer")))]),
    ("prop-named", [D("dev", N("n", P("p", "float", name="Power °")))]),
    ("prop-empty-name-defaults", [D("dev", N("n", P("p", "float", name="")))]),
    ("prop-format-string", [D("dev", N("n", P("p", "integer", format="0:100:5")))]),
    ("prop-format-empty-omitted", [D("dev", N("n", P("p", "integer", format="")))]),
    ("prop-format-json-schema-dict", [D("dev", N("n", P("req", "json", format=SCHEMA_FMT, settable=True)))]),
    ("prop-format-json-schema-string", [D("dev", N("n", P("req", "json", format='{"type":"array"}')))]),
    ("prop-settable", [D("dev", N("n", P("p", "boolean", settable=True)))]),
    ("prop-not-retained", [D("dev", N("n", P("p", "string", retained=False)))]),
    ("prop-settable-not-retained", [D("dev", N("n", P("brew", "boolean", settable=True, retained=False)))]),
    ("prop-unit", [D("dev", N("n", P("p", "float", unit="W")))]),
    ("prop-unit-degree-c", [D("dev", N("n", P("t", "float", unit="°C", format="-20:120")))]),
    ("prop-unit-mired", [D("dev", N("n", P("ct", "integer", unit="MK⁻¹")))]),
    ("prop-unit-empty-omitted", [D("dev", N("n", P("p", "float", unit="")))]),
    ("prop-every-option", [D("dev", N("n", P("p", "float", name="All", format="0:1:0.1", settable=True, retained=False, unit="%")))]),
    ("every-datatype", [D("dev", N("n",
        P("i", "integer"), P("f", "float"), P("b", "boolean"), P("s", "string"), P("e", "enum", format="a,b,c"),
        P("c", "color", format="rgb,hsv"), P("dt", "datetime"), P("du", "duration"), P("j", "json")))]),
    ("property-order", [D("dev", N("n", P("zeta", "integer"), P("alpha", "integer"), P("mid", "integer")))]),
    ("multiple-nodes-order", [D("dev", N("z-node", P("p", "integer")), N("a-node", P("q", "float")), N("m", P("r", "string")))]),
    ("extensions", [D("dev", type="t", extensions=["energy.ebus.imported", "org.example.feature"])]),
    ("extras-scalar", [D("dev", extras={"imported-from": "vendor-x"})]),
    ("extras-nested-and-float", [D("dev", type="t", extras={"z-extra": {"a": [1, 2.5], "b": {"c": None, "d": True}}, "a-extra": 0.1, "n": 1e16})]),
    ("extras-core-key-dropped", [D("dev", type="t", extras={"name": "evil", "root": "x", "type": "y", "keep": "k"})]),
    ("extras-unicode", [D("dev", extras={"label": "°C \U0001f600", "é": 1})]),
    ("child-one", [D("bridge", type="bridge"), D("relay", N("n", P("p", "boolean", settable=True)), type="relay", parent="bridge")]),
    ("child-tree-zwave", [D("bridge", name="Zwave bridge"), D("dualrelay", name="Zwave relay", parent="bridge"),
                         D("light1", name="First light", parent="dualrelay", extensions=["x.y.z"]),
                         D("light2", name="Second light", parent="dualrelay")]),
    ("children-order", [D("root"), D("c-3", parent="root"), D("c-1", parent="root"), D("c-2", parent="root")]),
    ("child-with-extras", [D("panel", type="energy.ebus.device.panel", extras={"vendor": "x"}),
                          D("circuit-1", N("power", P("w", "float", unit="W", name="Power")), parent="panel", type="energy.ebus.device.circuit",
                            extras={"imported-from": {"id": "c1", "source": "span"}})]),
    ("deep-chain", [D("a"), D("b", parent="a"), D("c", parent="b"), D("d", parent="c")]),
    ("full", [D("super-car", N("engine", P("temperature", "float", name="Engine temperature", unit="°C", format="-20:120"),
                               P("speed", "integer", unit="rpm"), name="Car engine", type="org.example.engine"),
                N("lights", P("color", "color", format="rgb", settable=True), P("on", "boolean", settable=True)),
                name="Supercar", type="org.example.car", extensions=["org.example.our-feature"], extras={"model-year": 2026})]),
]


def build_tree(spec: list) -> list:
    devices = {}
    order = []
    for d in spec:
        kwargs = {"id": d["id"], "name": d["name"], "type": d["type"], "extensions": d["extensions"],
                  "description_extras": d["extras"]}
        if d["parent"]:
            kwargs["parent"] = devices[d["parent"]]
        dev = Device(**kwargs)
        for n in d["nodes"]:
            node = Node(id=n["id"], name=n["name"], type=n["type"], device=dev)
            dev.add_node(node)
            for p in n["properties"]:
                node.add_property(Property(**p))
        devices[d["id"]] = dev
        order.append((dev, d))
    return order


def structural_input(dev: Device, spec: dict) -> dict:
    nodes = []
    for node_id, node in dev.nodes().items():
        props = []
        for prop_id, prop in node.properties().items():
            fmt = prop.format()
            props.append(
                {
                    "id": prop_id,
                    "name": prop.name(),
                    "datatype": str(prop.datatype()),
                    "format": json.dumps(fmt) if isinstance(fmt, dict) else fmt,
                    "settable": prop.settable(),
                    "retained": prop.retained(),
                    "unit": prop._unit,
                }
            )
        nodes.append({"id": node_id, "name": node.name(), "type": node.type(), "properties": props})
    return {
        "id": dev.id(),
        "name": dev.name(),
        "type": dev.type(),
        "nodes": nodes,
        "children": dev.children_ids(),
        "root": dev.root_id(),
        "parent": dev.parent_id(),
        "extensions": list(dev.extensions()),
        # Raw extras as passed to Python (core keys included: the serializer must drop them).
        "extras": [[k, v] for k, v in (spec["extras"] or {}).items()],
    }


def check_nested_sorted(value) -> None:
    """Nested extras objects must have sorted keys: serde_json's Map (no preserve_order) iterates sorted."""
    if isinstance(value, dict):
        assert list(value) == sorted(value), value
        for v in value.values():
            check_nested_sorted(v)
    elif isinstance(value, list):
        for v in value:
            check_nested_sorted(v)


def gen_description_wire() -> list:
    cases = []
    for label, spec in TREES:
        for dev, d in build_tree(spec):
            check_nested_sorted(list((d["extras"] or {}).values()))
            description = dev.description()
            wire = json.dumps(description)
            content_hash = Device._description_content_hash(description)
            cases.append(
                {
                    "id": f"description-wire-{label}-{dev.id()}",
                    "source": f"{PY} json.dumps(Device.description()), Device._description_content_hash",
                    "input": structural_input(dev, d),
                    "expect": {"wire": wire, "version": description["version"], "content_hash_hex": content_hash},
                }
            )
    return cases


# ── python-sdk: description-roundtrip ──────────────────────────────────────────
#
# Python's wire text for every description-wire tree, plus trees whose node,
# property, extra and child ids are all out of alphabetical order. A parser
# that reads objects into a sorted map fails these.

ROUNDTRIP_TREES = [
    ("order-everywhere", [D("dev",
        N("zeta", P("zeta", "integer"), P("alpha", "float"), P("mid", "string")),
        N("alpha", P("mid", "boolean"), P("zeta", "enum", format="b,a"), P("alpha", "json")),
        N("mid", P("alpha", "integer"), P("mid", "integer"), P("zeta", "integer")),
        type="t", extras={"zeta": 1, "alpha": {"a": 1, "b": [2.5, None]}, "mid": "m"})]),
    ("order-children", [D("root", N("zeta"), N("alpha")), D("zeta", parent="root"), D("alpha", parent="root"),
                        D("mid", N("zeta", P("zeta", "integer"), P("alpha", "integer")), parent="root")]),
]


def gen_description_roundtrip() -> list:
    cases = []
    for label, spec in TREES + ROUNDTRIP_TREES:
        for dev, d in build_tree(spec):
            check_nested_sorted(list((d["extras"] or {}).values()))
            wire = json.dumps(dev.description())
            cases.append(
                {
                    "id": f"description-roundtrip-{label}-{dev.id()}",
                    "source": f"{PY} json.dumps(Device.description())",
                    "input": {"payload": wire},
                    "expect": {"wire": wire},
                }
            )
    return cases


# ── main ───────────────────────────────────────────────────────────────────


def main() -> None:
    write_suite("topics.json", "topics", gen_topics())
    write_suite("empty-string.json", "empty-string", gen_empty_string())
    write_suite("effective-state.json", "effective-state", gen_effective_state())
    for dt, cases in gen_payloads().items():
        write_suite(f"payloads/{dt}.json", f"payloads/{dt}", cases)
    write_suite("description.json", "description", gen_description())
    write_suite("check-value.json", "check-value", gen_check_value())
    for name, gen in [
        ("float-format", gen_float_format),
        ("float32-format", gen_float32_format),
        ("sanitize-id", gen_sanitize_id),
        ("set-payload", gen_set_payload),
        ("json-dumps", gen_json_dumps),
        ("encode-duration", gen_encode_duration),
        ("description-wire", gen_description_wire),
        ("description-roundtrip", gen_description_roundtrip),
    ]:
        write_suite(f"{PYSDK}/{name}.json", f"{PYSDK}/{name}", gen())
    for path, n in COUNTS:
        print(f"{n:5d}  vectors/{path}")


if __name__ == "__main__":
    main()
