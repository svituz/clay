# Third party imports
from hl7apy.parser import get_message_info

# Communication Layer Imports
from . import Serializer
from ..exceptions import InvalidMessage


class AbstractHL7Serializer(Serializer):

    SERIALIZERS = {}

    def __init__(self, message_type, catalog):
        super(AbstractHL7Serializer, self).__init__(message_type)
        if self.__class__ == 'AbstractHL7Serializer':
            raise Exception("Cannot instantiate AbstractHL7Serializer directly. It is meant to be \
                            extended with the serializers dictionary")
        try:
            self.serializer = self.SERIALIZERS[message_type]()
        except KeyError:
            raise InvalidMessage

    def serialize(self, datum):
        return self.serializer.serialize(datum)

    @classmethod
    def deserialize(cls, message, catalog):
        # FIXME: we can avoid usage of hl7apy by copying the same function here
        enc_chars, msg_type, version = get_message_info(message)
        try:
            serializer = cls.SERIALIZERS[msg_type]
        except KeyError:
            raise InvalidMessage
        return serializer.deserialize(message, catalog)

# vim:tabstop=4:expandtab