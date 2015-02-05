import Queue

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer

# Clay library imports
from . import Messenger
from ..exceptions import MessengerError


class KafkaError(MessengerError):
    pass


class KafkaMessenger(Messenger):
    """
    This class implements a messenger specific for the Kafka broker.

    :type host: `string`
    :param host: the Kafka broker address

    :type port: `int`
    :param port: the Kafka server port
    """

    def __init__(self, host='localhost', port=9092):
        self.host = host
        self.port = port
        self._url = "{:s}:{:d}".format(self.host, self.port)

        self._spooling_queue = Queue.Queue()

        self._queues = {}

    def set_credentials(self, username, password):
        """
        .. warning::
            Kafka doesn't support authentication. This function is provided for interface completeness. It just rises a
            NotImplementedError exception!

        :type username: 'string'
        :param username: the username to use for the authentication

        :type password: 'string'
        :param password: the password for the given username

        :raises: NotImplementedError
        """
        raise NotImplementedError

    def add_queue(self, queue_name, durable, response):
        self._queues[queue_name] = {'durable': durable, 'response': response}
        return True

    def send(self, message):
        """
        Send the message.

        :type message: Message
        :param message: the message to send. It must be an object of the clay.Message class or a subclass that
            implements the serialize() method.
        """
        return self._send(message)

    def _send(self, message):
        result = None

        try:
            self._queues[message.domain]
        except KeyError:
            raise KafkaError("No queue specified for this message")

        try:
            routing_key = "{}-{}".format(message.domain, message.message_type)
            client = KafkaClient(self._url)
            producer = SimpleProducer(client)
            producer.send_messages(
                routing_key,
                message.serialize())
        except Exception as ex:
            self._spooling_queue.put(message)
            print "No connection, queuing"
            print "There are {0} messages in the queue".format(self._spooling_queue.qsize())
            print ex

        return result

# vim:tabstop=4:expandtab