class SchemaException(Exception):
    pass


class InvalidMessage(Exception):
    def __init__(self, message_type):
        self.message_type = message_type

    def __str__(self):
        return "Message %s is not valid" % self.message_type


class MissingParameters(Exception):
    def __str__(self):
        return "At least one query parameters must be present"


class MissingHandler(Exception):
    def __str__(self):
        return "The handler function must be set"


class AMQPError(Exception):
    pass

# vim:tabstop=4:expandtab
