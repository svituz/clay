from cStringIO import StringIO

# AVRO Imports
import avro.schema
from avro.io import DatumWriter, DatumReader, BinaryEncoder, BinaryDecoder, AvroTypeException

# Package Imports
from . import Serializer
from .. import schema_from_name, schema_from_id
from ..exceptions import SchemaException

ENVELOPE_SCHEMA = {
    "namespace": "TRACEABILITY",
    "name": "ENVELOPE",
    "type": "record",
    "fields": [
        {"name": "id",      "type": "int"},
        {"name": "payload", "type": "bytes"}
    ]
}


class AvroSerializer(Serializer):

    def __init__(self, message_type):
        schema_id, schema = schema_from_name(message_type)
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
    def deserialize(message, domain):
        envelope_schema = avro.schema.make_avsc_object(ENVELOPE_SCHEMA)
        envelope_reader = DatumReader(envelope_schema)
        envelope_decoder = BinaryDecoder(StringIO(message))
        envelope = envelope_reader.read(envelope_decoder)

        payload_id = envelope['id']
        payload_schema = schema_from_id(payload_id, domain)
        payload_avro_schema = avro.schema.make_avsc_object(payload_schema)
        payload_reader = DatumReader(payload_avro_schema)
        payload_decoder = BinaryDecoder(StringIO(envelope['payload']))
        payload = payload_reader.read(payload_decoder)

        return payload_id, payload, payload_schema

# vim:tabstop=4:expandtab