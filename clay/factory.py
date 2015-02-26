import clay
from .message import Message


class MessageFactory(object):
    """
    Create a factory for the messages of the types included in the given catalog and that should be serialized with
    the given serializer class.

    :type serializer: `class`
    :param serializer: the :class:`Serializer <clay.serializer.Serializer>` class

    :type catalog: `dict`
    :param catalog: the catalog with the schemas to use for the message creation and serialization/deserialization
    """
    __metaclass__ = clay.MessageFactoryMetaclass

    def __init__(self, serializer, catalog):
        self.serializer = serializer
        self.catalog = catalog
        clay.add_catalog(self.catalog)

    def create(self, message_type, content=None):
        """
        Create an instance of Message class of the given type and serialization strategy.

        If the content is given, the message is populated.

        :param message_type: the type of the message to be created.
         It must be the name of one of the schemas included in the factory catalog,
         otherwise an InvalidSchema Exception is raised.
        :type message_type: `str`

        :param content: if present, the message is populated with the content provided.
        :type content: `dict`

        :return: a :class:`Message <clay.message.Message>` instance

        >>> mf = MessageFactory(AvroSerializer, SINGLE_EXAMPLE_CATALOG)
        >>> m = mf.create("DEPOSIT")
        >>> m.timestamp = str(time.time)
        ...

        >>> m = mf.create("DEPOSIT", {'timestamp': str(time.time()), 'client_id': 'John Doe', 'atm_id': 'ROME_101', 'amount': 100})
        """
        msg = Message(message_type, self.catalog, self.serializer)
        msg.set_content(content)
        return msg

    def retrieve(self, message):
        """
        Retrieve the content from the serialized message and return a populated instance of the
        :class:`Message <clay.message.Message>` class.

        :param message: the serialized message to deserialize and retrieve
        :return: a populated instance of the :class:`Message <clay.message.Message>` class

        >>> mf = MessageFactory(AvroSerializer, TEST_CATALOG)
        >>> m = mf.retrieve('\\x00\\x10\\x8e\\xd1\\x87\\x01\\x06aaa')
        >>> m.id
        1111111
        >>> m.name
        "aaa"
        """
        payload, payload_id, payload_schema = self.serializer.deserialize(message, self.catalog)
        message = Message(payload_schema['name'], self.catalog, self.serializer)
        message.set_content(payload)

        return message

# vim:tabstop=4:expandtab