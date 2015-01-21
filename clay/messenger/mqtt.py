import Queue
import paho.mqtt.publish as MQTTPublisher
import paho.mqtt.client as MQTTPClient

# Clay library imports
from . import Messenger


class MQTTError(Exception):
    pass


class MQTTMessenger(Messenger):
    def __init__(self, host='localhost', port=1883):
        self.host = host
        self.port = port

        self._initialized = False

        self._spooling_queue = Queue.Queue()

        self._queues = {}
        self._credentials = None

    def set_credentials(self, username, password):
        self._credentials = {'username': username, 'password': password}

    def add_queue(self, queue_name, durable, response):
        self._queues[queue_name] = {'durable': durable, 'response': response}
        return True

    def send(self, message):
        return self._send(message)

    def _send(self, message):
        result = None

        try:
            self._queues[message.domain]
        except KeyError:
            raise MQTTError("No queue specified for this message")

        try:
            routing_key = "{}/{}".format(message.domain, message.message_type)
            MQTTPublisher.single(
                topic=routing_key,
                payload=message.serialize().encode('base64'),
                qos=1,
                hostname=self.host,
                port=self.port,
                auth=self._credentials,
                tls=None
            )
        except Exception as ex:
            # self._message_queue.put(message)
            # print "No connection, queuing"
            # print "There are {0} messages in the queue".format(self._message_queue.qsize())
            print ex

        return result


class MQTTBroker(object):
    # """
    # Class that implements a MQTT broker. The class creates a RabbitMQ *topic* exchange and start
    # consuming on the queue specified in input. The broker consumes every message with matching the
    # routing key <queue>.*
    #
    # :type host: `string`
    # :param host: the RabbitMQ server address
    #
    # :type port: `int`
    # :param port: the RabbitMQ MQTT Plugin server port
    # """
    def __init__(self, host='localhost', port=1883):
        self._host = host
        self._port = port
        self.handler = None

        self._client = MQTTPClient.Client()
        self._queue = None
        self._credentials = None

    def set_queue(self, queue_name, durable, response):
    #     """
    #     Set the queue whose messages the broker will consume. If response is `True` the counterpart
    #     consumer will receive the return value of the handler
    #
    #     :type queue_name: `string`
    #     :param queue_name: The name of the queue
    #     :type durable: `boolean`
    #     :param durable: It specifies if the queue should be durable or not
    #
    #     :type response: `boolean`
    #     :param response: If it's True then the Broker will return to the consumer the result of the
    #                      handler function
    #
    #     """
        self._queue = queue_name

    def set_credentials(self, username, password):
        self._credentials = {'username': username, 'password': password}

    def _handler_wrapper(self, client, userdata, message):
        self.handler(message.payload.decode('base64'), message.topic)

    def run(self):
        if self._credentials is not None:
            self._client.username_pw_set(
                self._credentials['username'],
                self._credentials['password']
            )

        self._client.on_message = self._handler_wrapper
        self._client.connect(host=self._host, port=self._port)

        self._client.subscribe(self._queue + '/#')
        self._client.loop_forever()

    def stop(self):
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception as ex:
            pass

    def __del__(self):
        self.stop()

# vim:tabstop=4:expandtab