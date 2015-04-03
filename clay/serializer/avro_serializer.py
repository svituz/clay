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

# AVRO Imports
import avro.schema
from avro.io import DatumWriter, DatumReader, BinaryEncoder, BinaryDecoder, AvroTypeException

# Package Imports
from . import Serializer
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


class AvroSerializer(Serializer):
    """
    Class to serialize and deserialize messages using Avro
    """

    def __init__(self, message_type, schema_catalog):
        schema_id, schema = schema_from_name(message_type, schema_catalog)
        self.payload_schema_id = schema_id

        self.payload_schema = avro.schema.make_avsc_object(schema)
        self.envelope_schema = avro.schema.make_avsc_object(ENVELOPE_SCHEMA)

        self.payload_encoder = None
        self.payload_writer = DatumWriter(self.payload_schema)

        self.envelope_encoder = None
        self.envelope_writer = DatumWriter(self.envelope_schema)

    def serialize(self, datum):
        self.payload_encoder = BinaryEncoder(StringIO())
        self.envelope_encoder = BinaryEncoder(StringIO())

        try:
            self.payload_writer.write(datum, self.payload_encoder)
            envelope_message = {
                'id': self.payload_schema_id,
                'payload': self.payload_encoder.writer.getvalue()
            }
            self.envelope_writer.write(envelope_message, self.envelope_encoder)
        except AvroTypeException as aex:
            raise SchemaException(datum)

        return self.envelope_encoder.writer.getvalue()

    @staticmethod
    def deserialize(message, catalog):
        envelope_schema = avro.schema.make_avsc_object(ENVELOPE_SCHEMA)
        envelope_reader = DatumReader(envelope_schema)
        envelope_decoder = BinaryDecoder(StringIO(message))
        envelope = envelope_reader.read(envelope_decoder)

        payload_id = envelope['id']
        payload_schema = catalog[payload_id]
        payload_avro_schema = avro.schema.make_avsc_object(payload_schema)
        payload_reader = DatumReader(payload_avro_schema)
        payload_decoder = BinaryDecoder(StringIO(envelope['payload']))
        payload = payload_reader.read(payload_decoder)

        return payload, payload_id, payload_schema

# vim:tabstop=4:expandtab