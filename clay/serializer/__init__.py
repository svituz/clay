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

import simplejson


class Serializer(object):
    """
    Base Serializer class. This class is just an interface for serializers.
    All Serializers receive the message_type of the message to serialize and the catalog containing the structure of the
    message

    :type message_type: `str`
    :param message_type: The message type of the message to serialize

    :type schema_catalog:
    :param schema_catalog: The catalog containing the schema of the message to serialize
    """
    def __init__(self, message_type, schema_catalog):
        pass

    def serialize(self, datum):
        """
        Method where the serialization is performed. Sublclasses should implement this method

        :type datum: `dict`
        :param datum: The dictionary containing the value of the message to serialize. The dictionary must be compatible
            with the schema of the message
        """
        pass

    @staticmethod
    def deserialize(message, catalog):
        """
        Static or class method that receives a serialized message and the catalog containing the schema of the
        message and create a :class:`Message <clay.message.Message>` object from it

        :param message: The serialized message
        :param catalog: The catalog containing the message schema
        :return: :class:`Message <clay.message.Message>`
        """
        pass


class JSONSerializer(Serializer):
    def __init__(self, message_type, catalog):
        pass

    def serialize(self, datum):
        return simplejson.dumps(datum)

    @staticmethod
    def deserialize(message, catalog):
        pass


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
    from .hl7_serializer import AbstractHL7Serializer
except ImportError:
    pass
