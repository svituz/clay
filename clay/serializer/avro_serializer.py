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

from cStringIO import StringIO

from ..exceptions import MissingDependency

# AVRO Imports
try:
    import avro.schema
except ImportError:
    raise MissingDependency("avro")

from avro.io import DatumWriter, DatumReader, BinaryEncoder, BinaryDecoder, AvroTypeException

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


class AvroCache(Cache):

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
                schema_obj = avro.schema.make_avsc_object(schema)
                obj = DatumWriter(schema_obj)
            else:
                schema_obj = avro.schema.make_avsc_object(schema)
                obj = DatumReader(schema_obj)
            cache[ser_name] = obj
        return obj


class CustomEncoder(BinaryEncoder):
    # This encoder fix problems with utf8 characters
    def write_utf8(self, datum):
        try:
            self.write_bytes(datum.encode("utf-8"))
        except UnicodeDecodeError:
            self.write_bytes(unicode(datum, "utf-8").encode("utf-8"))


class AvroSerializer(Serializer):
    """
    Class to serialize and deserialize messages using Avro
    """

    def __init__(self, message_type, schema_catalog):
        schema_id, schema = schema_from_name(message_type, schema_catalog)
        self.payload_schema_id = schema_id

        self._payload_writer = AvroCache().get(AvroCache.SER, schema)
        self._envelope_writer = AvroCache().get(AvroCache.SER, ENVELOPE_SCHEMA)

    def serialize(self, datum):
        payload_encoder = CustomEncoder(StringIO())
        envelope_encoder = BinaryEncoder(StringIO())

        try:
            self._payload_writer.write(datum, payload_encoder)
            envelope_message = {
                "id": self.payload_schema_id,
                "payload": payload_encoder.writer.getvalue()
            }
            self._envelope_writer.write(envelope_message, envelope_encoder)
        except AvroTypeException:
            raise SchemaException(datum)

        return envelope_encoder.writer.getvalue()

    @staticmethod
    def deserialize(message, catalog):
        envelope_reader = AvroCache().get(AvroCache.DESER, ENVELOPE_SCHEMA)
        envelope_decoder = BinaryDecoder(StringIO(message))
        envelope = envelope_reader.read(envelope_decoder)

        payload_id = envelope["id"]
        payload_schema = catalog[payload_id]
        payload_reader = AvroCache().get(AvroCache.DESER, payload_schema)
        payload_decoder = BinaryDecoder(StringIO(envelope["payload"]))
        payload = payload_reader.read(payload_decoder)

        return payload, payload_id, payload_schema