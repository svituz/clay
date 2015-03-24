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

from collections import MutableMapping

from . import schema_from_name
from .exceptions import SchemaException, InvalidMessage
from .serializer import DummySerializer


def _is_primitive_type(t):
    primitive_types = ("null", "boolean", "int", "long",
                       "float", "double", "bytes", "string")
    if isinstance(t, list):
        for t in t:
            if t not in primitive_types:
                return False
        return True
    else:
        return t in primitive_types


class _Item(object):
    def __init__(self, fields):
        self._attrs = []

        # add field names to _attrs list
        for field in fields:
            self._attrs.append(field["name"])
            if isinstance(field["type"], list):
                # We allow only two kind of types
                # FIXME: we are taking that the list contains only two types
                if "null" not in field["type"]:
                    raise SchemaException("The schema structure is not valid: found more than one \
                                          field type")
                else:
                    field_type = [t for t in field["type"] if t != "null"][0]
            else:
                field_type = field["type"]

            if _is_primitive_type(field_type):
                setattr(self, field["name"], field.get("default"))
            elif isinstance(field_type, MutableMapping):
                if field_type["type"] == "array":
                    setattr(self, field["name"], _Array(field_type["items"]))
                else:
                    setattr(self, field["name"], _Item(field_type["fields"]))

    def as_obj(self):
        d = {}
        for attr in self._attrs:
            value = getattr(self, attr)
            try:
                d[attr] = value.as_obj()
            except AttributeError:
                d[attr] = value
        return d

    def set_content(self, content):
        for k, v in content.iteritems():
            try:
                setattr(self, k, v)
            except ValueError:  # complex datatype
                attr = getattr(self, k)
                attr.set_content(v)

    def __setattr__(self, key, value):
        if key == '_attrs' or key in self._attrs:
            try:
                to_be_set = getattr(self, key)
            except AttributeError:
                pass
            else:
                if isinstance(to_be_set, _Array) or isinstance(to_be_set, _Item):
                    raise ValueError("Cannot assign field of complex type")
            super(_Item, self).__setattr__(key, value)
        else:
            raise AttributeError("%r object has no attribute %r" % (self.__class__.__name__, key))

    def __repr__(self):
        return repr(self.as_obj())


class _Array(object):
    def __init__(self, fields):
        self._list = []
        if _is_primitive_type(fields):
            self.fields = fields
        else:
            self.fields = fields['fields']

    def add(self, content=None):
        if _is_primitive_type(self.fields):
            item = content
            self._list.append(content)
        else:
            item = _Item(self.fields)
            if content:
                item.set_content(content)
            self._list.append(item)
        return item

    def set_content(self, content):
        for item in content:
            self.add(item)

    def as_obj(self):
        if _is_primitive_type(self.fields):
            return self._list
        else:
            d = []
            for item in self._list:
                d.append(item.as_obj())
            return d

    def __setitem__(self, index, item):
        self._list[index] = item

    def __getitem__(self, index):
        return self._list[index]

    def __delitem__(self, index):
        del self._list[index]

    def __len__(self):
        return len(self._list)

    def __repr__(self):
        return repr(self._list)


class Message(object):
    """
    This class represents a CLay message. A message is identified by a type which indicates its structure
    inside the catalog. Once a message is created, it is possible to navigate it, accessing to its fields as
    Python attributes.

    The fields can be simple or complex. The simple fields' types are the same as Avro's ones: null, boolean, int, long,
    float, double, bytes and string; the complex can be Avro arrays (Python lists) or records (Python dictionaries).
    It is possible to directly assign a value only to simple fields. In case of complex fields, we need to distinguish
    between the two cases: for records it is possible to discend the fields starting from the parent, in case of lists
    you will need to explicitly add an item, using the :meth:`add` method, and then access to the correct occurrence
    by index (the :meth:`add` method)

    For example, consider a message that has this simple schema:

    .. code:: python

        schema = {
            "namespace": "TEST", "name": "TEST", "type": "record",
            "fields": [
                {"name": "field_1", "type": "int"},
                {"name": "field_2", "type": {
                    "type": "record", "fields": [{"name": "field_2_1", "type": "boolean"}]
                }},
                {"name": "field_3", "type": {
                    "type": "array", "items": {
                        "type: "record", "name": "FIELD_3",
                        "fields": [{"name": "field_3_1, "type": "string"}]
                    }
                }}
            ]
        }

    Here follows the code to populate the message

    .. code:: python

        # simple field: direct assignment
        msg.field_1 = 1  # simple field

        # record: in case of record we can discend the message
        msg.field_2.field_2_1 = True

        # array: in case of array we need to create the instance and then assign value to the child
        f = msg.field_3.add()  # first we create the instance using the add method
        f.field_3_1 = "val"  # then it is possible to navigate the new item

    .. warning::
        The type of the value assigned must be compliant with the type defined in the message schema for the particular
        field. If it doesn't, an error will be raised but only during the serialization phase.

    A message needs also a :class:`Serializer <clay.serializer.Serializer>` used to represent it in a particular format.

    .. important::
      It is strongly recommended to create a message using a :class:`MessageFactory <clay.factory.MessageFactory>`
      instead of instantiating a :class:`Message` directly.

    :type message_type: `str`
    :param message_type: The type of message. It has to be a valid message type for the :attr:`catalog`
    :type catalog: `dict`
    :param catalog: The catalog containing the structure of the message
    :type serializer: `class`
    :param serializer: the :class:`Serializer <clay.serializer.Serializer>` class to use to serialize the message
    """
    def __init__(self, message_type, catalog, serializer=DummySerializer):
        try:
            self.schema = schema_from_name(message_type, catalog)[1]
        except SchemaException:
            raise InvalidMessage(message_type)

        self._message_type = message_type
        self._domain = self.schema["namespace"]
        self._serializer = serializer(message_type, catalog)
        self._struct = _Item(self.schema["fields"])

    domain = property(lambda self: self._domain, doc="The domain of the message in the catalog")
    message_type = property(lambda self: self._message_type, doc="The message type")
    fields = property(lambda self: self._struct.as_obj(), doc="The dictionary representation of the message")

    def serialize(self):
        """
        Serializes the message using the :class:`Serializer <clay.serializer.Serializer>`

        :rtype: `str`
        :return: The serialized message
        """
        return self._serializer.serialize(self._struct.as_obj())

    def set_content(self, content=None):
        """
        Assign the values to the message fields using a dictionary in input. The dictionary must follow the correct
        structure specified in the catalog

        :type content: `dict`
        :param content: the `dict` object with the values of the :class:`Message`'s fields


        """
        if content is not None:
            self._struct.set_content(content)

    def __setattr__(self, name, value):
        try:
            setattr(self._struct, name, value)
        except AttributeError:
            super(Message, self).__setattr__(name, value)

    def __getattr__(self, name):
        try:
            return getattr(self._struct, name)
        except:
            raise AttributeError("%r object has no attribute %r" % (self.__class__.__name__, name))

# vim:tabstop=4:expandtab
