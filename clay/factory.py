from clay.message import Message


class MessageFactory(object):

    def __init__(self, serializer):
        self.serializer = serializer

    def create(self, message_type):
        return Message(self.serializer, message_type)

    def retrieve(self, message):
        pass

# vim:tabstop=4:expandtab