from collections import MutableMapping

import clay
from .message import Message


class MessageFactory(object):
    __metaclass__ = clay.MessageFactoryMetaclass

    def __init__(self, serializer, catalog):
        self.serializer = serializer
        self.catalog = catalog
        clay.add_catalog(self.catalog)

    def create(self, message_type, content=None):
        msg = Message(message_type, self.catalog, self.serializer)
        msg.set_content(content)
        return msg

    def retrieve(self, message):
        def _fill_obj(obj, payload):
            for k, v in payload.iteritems():
                try:
                    setattr(obj, k, v)
                except ValueError:  # complex datatype
                    attr = getattr(obj, k)
                    for index, item in enumerate(v):
                        attr.add()
                        if isinstance(item, MutableMapping):
                            _fill_obj(attr[index], item)
                        else:
                            attr[index] = item

        payload, payload_id, payload_schema = self.serializer.deserialize(message, self.catalog)
        message = Message(payload_schema['name'], self.catalog, self.serializer)
        _fill_obj(message, payload)

        return message

# vim:tabstop=4:expandtab