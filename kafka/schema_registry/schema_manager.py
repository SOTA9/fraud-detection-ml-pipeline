"""
Schema Manager CLI
Usage:
python -m kafka.schema_registry.schema_manager info
python -m kafka.schema_registry.schema_manager versions
python -m kafka.schema_registry.schema_manager register
python -m kafka.schema_registry.schema_manager check-compat path/to/new.avsc
"""

import sys
import json
import os
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from kafka.schema_registry.registry_client import (
    SCHEMA_REGISTRY_URL,
    SUBJECT,
    SCHEMA_FILE,
    register_schema,
    get_schema_id,
    get_schema_version,
    list_all_versions
)

client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

def cmd_info():
    """Print latest schema info and definition"""
    sid, ver = get_schema_id(), get_schema_version()
    print(f"\nSchema Registry : {SCHEMA_REGISTRY_URL}")
    print(f"Subject : {SUBJECT}")
    print(f"Latest schema_id: {sid}")
    print(f"Latest version : {ver}\n")
    latest = client.get_latest_version(SUBJECT)
    print(json.dumps(json.loads(latest.schema.schema_str), indent=2))

def cmd_versions():
    """Print all versions of the schema"""
    print(f"\nAll versions for {SUBJECT!r}:")
    for v in list_all_versions():
        print(f" version={v['version']} schema_id={v['schema_id']}")

def cmd_register():
    """Register the schema file in Schema Registry"""
    if not os.path.exists(SCHEMA_FILE):
        print(f"Schema file not found: {SCHEMA_FILE}")
        sys.exit(1)
    sid = register_schema()
    print(f"Registered schema_id={sid} version={get_schema_version()}")

def cmd_check_compat(path: str):
    """Check compatibility of a new schema file"""
    if not os.path.exists(path):
        print(f"Schema file not found: {path}")
        sys.exit(1)

    schema = Schema(open(path).read(), schema_type="AVRO")
    ok = client.test_compatibility(SUBJECT, schema)
    print("COMPATIBLE" if ok else "INCOMPATIBLE")
    if not ok:
        sys.exit(1)

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "info"

    commands = {
        "info": cmd_info,
        "versions": cmd_versions,
        "register": cmd_register
    }

    # Run main commands
    if cmd in commands:
        commands[cmd]()
    # check-compat requires a file path
    elif cmd == "check-compat":
        if len(sys.argv) < 3:
            print("Usage: python -m kafka.schema_registry.schema_manager check-compat path/to/new.avsc")
            sys.exit(1)
        cmd_check_compat(sys.argv[2])
    else:
        print(__doc__)