from collections import MutableMapping

from clay import schema_from_name
from clay.exceptions import SchemaException, InvalidMessage


class _item(object):
    def __init__(self, fields):
        self._attrs = []
        for field in fields:
            self._attrs.append(field["name"])
            if isinstance(field["type"], list):
                # We allow only two kind of types
                if "null" not in field["type"]:
                    raise SchemaException("The schema structure is not valid: found more than one \
                                          field type")
                else:
                    field_type = [t for t in field["type"] if t != "null"][0]
            else:
                field_type = field["type"]

            if self._is_primitive_type(field_type):
                setattr(self, field["name"], None)
            elif isinstance(field_type, MutableMapping):
                setattr(self, field["name"], _array(field_type["items"]["fields"]))

    def as_obj(self):
        d = {}
        for attr in self._attrs:
            value = getattr(self, attr)
            try:
                d[attr] = value.as_obj()
            except AttributeError:
                d[attr] = value
        return d

    def __setattr__(self, key, value):
        if key == '_attrs' or key in self._attrs:
            super(_item, self).__setattr__(key, value)
        else:
            raise AttributeError("%r object has no attribute %r" % (self.__class__.__name__, key))

    def _is_primitive_type(self, type):
        primitive_types = ("null", "boolean", "int", "long",
                           "float", "double", "bytes", "string")
        if isinstance(type, list):
            for t in type:
                if t not in primitive_types:
                    return False
            return True
        else:
            return type in primitive_types


class _array(object):
    def __init__(self, fields):
        self._list = []
        self.fields = fields

    def add(self):
        self._list.append(_item(self.fields))

    def as_obj(self):
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


class Message(object):
    def __init__(self, serializer, message_type):
        try:
            self.schema = schema_from_name(message_type)[1]
        except SchemaException:
            raise InvalidMessage(message_type)

        self.message_type = message_type
        self.domain = self.schema["namespace"]
        self.serializer = serializer(message_type)
        self.struct = _item(self.schema["fields"])

    def serialize(self):
        return self.serializer.serialize(self.struct.as_obj())

    def __setattr__(self, name, value):
        try:
            setattr(self.struct, name, value)
        except:
            super(Message, self).__setattr__(name, value)

    def __getattr__(self, name):
        try:
            return getattr(self.struct, name)
        except:
            raise AttributeError("%r object has no attribute %r" % (self.__class__.__name__, name))

# vim:tabstop=4:expandtab
