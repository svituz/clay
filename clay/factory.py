from clay.message import Message


class MessageFactory(object):
    DOMAIN = None

    def __init__(self, serializer):
        self.serializer = serializer

    def create(self, message_type):
        return Message(self.serializer, message_type)

    def retrieve(self, message):
        payload, payload_id, payload_schema = self.serializer.deserialize(message, self.DOMAIN)

        message = Message(self.serializer, payload_schema['name'])
        for k, v in payload.iteritems():
            setattr(message, k, v)

        return message

# vim:tabstop=4:expandtab