RABBIT_EXCHANGE = 'test_exchange'

RABBIT_QUEUE = 'TESTS'

TEST_SCHEMA = {
    "namespace": RABBIT_QUEUE,
    "name": "TEST",
    "type": "record",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
    ]
}

TEST_COMPLEX_SCHEMA = {
    "namespace": RABBIT_QUEUE,
    "name": "TEST_COMPLEX",
    "type": "record",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "items",
         "type": {
             "type": "array",
             "items": {
                 "type": "record",
                     "name": "items_type",
                     "fields": [
                         {"name": "name", "type": "string"},
                     ]
                 }}},
        {"name": "simple_items",
         "type": {
             "type": "array",
             "items": "string"}}
    ]
}

TEST_WRONG_SCHEMA = {
    "namespace": RABBIT_QUEUE,
    "name": "TEST_WRONG",
    "type": "record",
    "fields": [
        {"name": "id", "type": ["int", "string"]},
        {"name": "name", "type": "string"},
    ]
}

TEST_CATALOG = {
    "version": 1,
    "name": "TEST_CATALOG",
    0: TEST_SCHEMA,
    1: TEST_COMPLEX_SCHEMA,
    2: TEST_WRONG_SCHEMA
}