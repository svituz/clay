# Builtins Imports
import os
import uuid
from datetime import datetime

# Communication Layer Imports
from . import Serializer
from ..exceptions import InvalidMessage


class AbstractHL7Serializer(Serializer):

    def __init__(self, message_type):
        if self.__class__ == 'AbstractHL7Serializer':
            raise Exception("Cannot instantiate AbstractHL7Serializer directly. It is meant to be extended with the \
                            serializers dictionary")
        try:
            self.serializer = self.serializers[message_type]()
        except AttributeError:
            raise Exception("Missing serializers dictionary. Specify it in the subclass")
        except KeyError:
            raise InvalidMessage

    def serialize(self, datum):
        return self.serializer.serialize(datum)

    def deserialize(self, message, domain):
        return self.serializer.deserialize(message)

# vim:tabstop=4:expandtab