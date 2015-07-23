# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2015, CRS4
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

from ..exceptions import MissingDependency

import simplejson

# AVRO Imports
try:
    import pyavroc
except ImportError:
    raise MissingDependency("pyavroc")

# Package Imports
from . import Serializer, Cache
from .. import schema_from_name
from ..exceptions import SchemaException

ENVELOPE_SCHEMA = {
    "namespace": "CLAY",
    "name": "ENVELOPE",
    "type": "record",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "payload", "type": "bytes"}
    ]
}


class PyAvrocCache(Cache):

    SER = 0
    DESER = 1

    def get(self, obj_type, schema):
        assert obj_type in (self.SER, self.DESER)
        cache = self._cache[obj_type]
        ser_name = schema["name"]
        try:
            obj = cache[ser_name]
        except KeyError:
            if obj_type == self.SER:
                obj = pyavroc.AvroSerializer(simplejson.dumps(schema))
            else:
                obj = pyavroc.AvroDeserializer(simplejson.dumps(schema))
            self._cache[ser_name] = obj
        return obj


class AvroSerializer(Serializer):
    """
    Class to serialize and deserialize messages using Avro
    """

    def __init__(self, message_type, schema_catalog):
        schema_id, schema = schema_from_name(message_type, schema_catalog)
        self.payload_schema_id = schema_id

        self._payload_ser = PyAvrocCache().get(PyAvrocCache.SER, schema)
        self._envelope_ser = PyAvrocCache().get(PyAvrocCache.SER, ENVELOPE_SCHEMA)

    def serialize(self, datum):
        try:
            payload = self._payload_ser.serialize(datum)
        except (IOError, TypeError) as e:
            raise SchemaException(datum)
        obj = {"id": self.payload_schema_id, "payload": payload}
        res = self._envelope_ser.serialize(obj)
        return res

    @staticmethod
    def deserialize(message, catalog):
        envelope_deser = PyAvrocCache().get(PyAvrocCache.DESER, ENVELOPE_SCHEMA)
        envelope = envelope_deser.deserialize(message)

        payload_id = envelope["id"]
        payload_schema = catalog[payload_id]

        payload_deser = PyAvrocCache().get(PyAvrocCache.DESER, payload_schema)
        payload = payload_deser.deserialize(envelope["payload"])

        return payload, payload_id, payload_schema

# vim:tabstop=4:expandtab