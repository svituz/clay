import simplejson


class Serializer(object):
    def __init__(self, message_type):
        pass

    def serialize(self, datum):
        pass


class JSONSerializer(Serializer):
    def __init__(self, message_type):
        pass

    def serialize(self, datum):
        return simplejson.dumps(datum)


class DummySerializer(Serializer):
    def __init__(self, message_type):
        pass

    def serialize(self, datum):
        return datum

# Imports the other Serializers
from .avro_serializer import AvroSerializer
from .hl7_serializer import AbstractHL7Serializer
