class SchemaException(Exception):
    pass


class InvalidMessage(Exception):
    def __init__(self, message_type):
        self.message_type = message_type

    def __str__(self):
        return "Message %s is not valid" % self.message_type


class MessengerError(Exception):
    pass

# vim:tabstop=4:expandtab
