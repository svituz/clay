Tutorial
========
This tutorial will explain with a working example the fundamental topics of developing with CLay:

 * Schemas Catalog
 * Message Factory
 * Messengers and Receivers

Catalog
-------

The catalog is a dictionary that contains the definitions of messages' schemas used to create the messages.
Every type of message that will be sent in your application using CLay, must have its schema defined in the catalog.
A catalog must have two mandatory keys `name` and `version` and an integer keys for every message schema.

An example of a catalog is the following:

.. code-block:: python

    catalog = {
        "name": "MY_CATALOG",
        "version": 1,
        0: MSG_1,
        1: MSG_2
    }

Messages Schemas
++++++++++++++++

The messages' schemas define the structure of the messages of your application. The CLay schemas are defined using a
subset of `Avro 1.7.7 <https://avro.apache.org/>`_ schemas' rules. For a complete tutorial about Avro schemas, refer to
Avro documentation. Here we describe only the features used in CLay.

A message schema must always start with a record (i.e., a Python `dict`). The first record has two very important keys:

 * the **namespace**: the namespace indicates the domain of the message. The namespaces are used by messages broker to
   route the messages correctly
 * the **name**: it is the message name. When you want to create a message of this type you need to specify this name

.. note::

  Apart the first record, the other records definition doesn't use the *namespace* and *name* keys. Even so, the latter
  must be specified and must be unique because Avro needs it.

The other fundamental key, of course, is **fields**, which is the list of the message's fields. Evey field, as for Avro,
needs a **name** and a **type**. CLay messages support all Avro primitive types (*null*, *boolean*, *int*, *long*,
*float*, *double*, *bytes* and *string*) but only *records* and *arrays* complex datatypes.
Unlike Avro, a CLay field can only be of one type; the only exception to this
rule is for null datatype: if it's present in the file type definition, it means that the field can be ``None``.

Here follows an example of a CLay legal schema

.. code-block:: python

  EXAMPLE_MESSAGE = {
    "namespace": "EXAMPLE",
    "name": "MSG_1",
    "type": "record",
    "fields": [
        {"name": "FIELD_1", "type": ["string", "null"]},  # primitive type. It can be null (i.e., None)
        {"name": "FIELD_2", "type": {
            "type": "record", "name": "FIELD_2_TYPE",
            "fields": [
                {"name": "FIELD_2_1", "type": "int"},
                {"name": "FIELD_2_2", "type": "double"}
            ]
        }},
        {"name": "FIELD_3", "type": {  # array with primitive types items
            "type": "array",
            "items": "string"
        }},
        {"name": "FIELD_4", "type": [{  # array with complex types items
            "type": "array",
            "items": {
                "type": "record", "name": "FIELD_4_TYPE",
                "fields": [
                    {"name": "FIELD_4_1", "type": ["float", "null"]},
                    {"name": "FIELD_4_2", "type": ["string", "null"]}
                ]
            }
        }, "null"]},  # it can be null
    ]
  }