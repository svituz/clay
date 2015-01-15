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
try:
    from .avro_serializer import AvroSerializer
except ImportError:
    pass

try:
    from .hl7_serializer import HL7Serializer
except ImportError:
    pass
