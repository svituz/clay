import clay
from .message import Message


class MessageFactory(object):
    def __init__(self, serializer, catalog):
        self.serializer = serializer
        self.catalog = catalog
        clay.add_catalog(self.catalog)

    def create(self, message_type):
        return Message(message_type, self.catalog, self.serializer)

    def retrieve(self, message):
        payload, payload_id, payload_schema = self.serializer.deserialize(message, self.catalog)

        message = Message(payload_schema['name'], self.catalog, self.serializer)
        for k, v in payload.iteritems():
            setattr(message, k, v)

        return message

# vim:tabstop=4:expandtab