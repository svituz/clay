# This file contains the schemas and catalog common to the examples.

WITHDRAWAL_SCHEMA = {
    "namespace": "EXAMPLES",
    "name": "WITHDRAWAL",
    "type": "record",
    "fields": [
        {"name": "timestamp", "type": "string"},
        {"name": "client_id", "type": "string"},
        {"name": "atm_id",    "type": "string"},
        {"name": "amount",    "type": "int"}
    ]
}

DEPOSIT_SCHEMA = {
    "namespace": "EXAMPLES",
    "name": "DEPOSIT",
    "type": "record",
    "fields": [
        {"name": "timestamp", "type": "string"},
        {"name": "client_id", "type": "string"},
        {"name": "atm_id",    "type": "string"},
        {"name": "amount",    "type": "int"}
    ]
}

SINGLE_EXAMPLE_CATALOG = {
    "version": 1,
    "name": "SINGLE_EXAMPLE_CATALOG",
    0: WITHDRAWAL_SCHEMA,
    1: DEPOSIT_SCHEMA
}
