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

# Third party imports
from hl7apy.parser import get_message_info

# Communication Layer Imports
from . import Serializer
from ..exceptions import InvalidMessage


class AbstractHL7Serializer(Serializer):
    """
    Serializer for HL7 messages. The class cannot be instantiated and is meant to be extended.
    Subclasses just need to specify the :attr:`SERIALIZERS <clay.serializer.AbstractHL7Serializer.SERIALIZERS>` `dict`.
    """

    #: A class attribute `dict` that indicates the actual serializers in charge of the serialization.
    #: The items are pairs of message_type: :class:`Serializer <clay.serializer.Serializer>`. Subclasses must
    #: populate this `dict`
    SERIALIZERS = {}

    def __init__(self, message_type, schema_catalog):
        super(AbstractHL7Serializer, self).__init__(message_type)
        if self.__class__ == "AbstractHL7Serializer":
            raise Exception("Cannot instantiate AbstractHL7Serializer directly. It is meant to be \
                            extended with the serializers dictionary")
        try:
            self.serializer = self.SERIALIZERS[message_type]()
        except KeyError:
            raise InvalidMessage(message_type)

    def serialize(self, datum):
        return self.serializer.serialize(datum)

    @classmethod
    def deserialize(cls, message, catalog):
        # FIXME: we can avoid usage of hl7apy by copying the same function here
        enc_chars, msg_type, version = get_message_info(message)
        try:
            serializer = cls.SERIALIZERS[msg_type]
        except KeyError:
            raise InvalidMessage(msg_type)
        return serializer.deserialize(message, catalog)

# vim:tabstop=4:expandtab
