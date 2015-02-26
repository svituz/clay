class SchemaException(Exception):
    pass


class InvalidMessage(Exception):
    """
    Raised when the message type does not exists in the catalog
    """
    def __init__(self, message_type):
        self.message_type = message_type

    def __str__(self):
        return "Message %s is not valid" % self.message_type


class MessengerError(Exception):
    """
    Generic exception raised when a messenger error occurs
    """
    pass

# vim:tabstop=4:expandtab
