import Queue

import pika
from pika.exceptions import AMQPConnectionError, ChannelClosed

# Clay library imports
from . import Messenger
from clay.exceptions import AMQPError


class AMQPMessenger(Messenger):
    def __init__(self, host='localhost', port=5672):
        self.host = host
        self.port = port

        self._initialized = False

        self._message_queue = Queue.Queue()

        self._exchange = None
        self._queues = {}
        self._response = None

    def _set_exchange(self, exchange):
        self._exchange = exchange

    def _get_exchange(self):
        return self._exchange

    exchange = property(_get_exchange, _set_exchange, doc="The RabbitMQ Topic Exchange property")

    def add_queue(self, queue_name, durable, response):
        self._queues[queue_name] = {'durable': durable, 'response': response}
        return True

    def send(self, message):
        return self._send(message)

    def _synchronous_callback(self, channel, method, properties, body):
        self._response = body

    def _send(self, message):
        result = None

        try:
            self._queues[message.domain]
        except KeyError:
            raise AMQPError("No queue specified for this message")

        try:
            routing_key = "{}.{}".format(message.domain, message.message_type)

            conn_param = pika.ConnectionParameters(host=self.host, port=self.port)
            connection = pika.BlockingConnection(conn_param)
            channel = connection.channel()


            # Checks if the queue is declared, but does not create it if not exists
            channel.queue_declare(queue=message.domain, passive=True)

            if self._queues[message.domain]['response'] is True:
                result = channel.queue_declare(exclusive=True)
                callback_queue = result.method.queue

                channel.basic_consume(self._synchronous_callback,
                                      queue=callback_queue,
                                      no_ack=True)

                channel.basic_publish(
                    exchange=self.exchange,
                    routing_key=routing_key,
                    body=message.serialize(),
                    mandatory=True,
                    properties=pika.BasicProperties(
                        reply_to=callback_queue
                    )
                )

                # wait for the answer
                while self._response is None:
                    connection.process_data_events()
                result = self._response
                self._response = None

            else:
                channel.basic_publish(
                    exchange=self.exchange,
                    routing_key=routing_key,
                    body=message.serialize(),
                    mandatory=True,
                    properties=pika.BasicProperties(
                        delivery_mode=2
                    )
                )

            connection.close()
        except (AMQPConnectionError, ChannelClosed) as amx:
            self._message_queue.put(message)
            print "No connection, queuing"
            print "There are {0} messages in the queue".format(self._message_queue.qsize())

        return result


class AMQPBroker(object):
    """
    Class that implements an AMQP broker. The class creates a RabbitMQ *topic* exchange and start
    consuming on the queue specified in input. The broker consumes every message with matching the
    routing key <queue>.*

    :type host: `string`
    :param host: the RabbitMQ server address

    :type port: `int`
    :param port: the RabbitMQ server port
    """
    def __init__(self, host='localhost', port=5672):
        self.host = host
        self.port = port

        self._channel = None
        self._connection = None

        self._exchange = None
        self._queue = None
        self.handler = None

    def _set_exchange(self, exchange):
        self._exchange = exchange
        conn_param = pika.ConnectionParameters(host=self.host, port=self.port)

        connection = pika.BlockingConnection(conn_param)
        channel = connection.channel()

        channel.exchange_declare(self.exchange, type='topic', durable=True)
        connection.close()

    def _get_exchange(self):
        return self._exchange

    exchange = property(_get_exchange, _set_exchange, doc="The RabbitMQ Topic Exchange property")

    def set_queue(self, queue_name, durable, response):
        """
        Set the queue whose messages the broker will consume. If response is `True` the counterpart
        consumer will receive the return value of the handler

        :type queue_name: `string`
        :param queue_name: The name of the queue
        :type durable: `boolean`
        :param durable: It specifies if the queue should be durable or not

        :type response: `boolean`
        :param response: If it's True then the Broker will return to the consumer the result of the
                         handler function

        """
        self._queue = {'name': queue_name, 'durable': durable, 'response': response}

    def _handler_wrapper(self, channel, method, properties, body):

        message_type = method.routing_key.split('.')[-1]
        res = self.handler(body, message_type)
        if self._queue['response'] is True:
            channel.basic_publish('', routing_key=properties.reply_to, body=res)

    def run(self):
        conn_param = pika.ConnectionParameters(host=self.host, port=self.port)
        self._connection = pika.BlockingConnection(conn_param)
        self._channel = self._connection.channel()

        self._channel.queue_declare(
            queue=self._queue['name'],
            durable=self._queue['durable'])

        self._channel.queue_bind(
            exchange=self.exchange,
            queue=self._queue['name'],
            routing_key="{}.*".format(self._queue['name']))

        # channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(self._handler_wrapper, queue=self._queue['name'], no_ack=True)

        self._channel.start_consuming()

    def stop(self):
        try:
            self._channel.stop_consuming()
            self._connection.close()
        except:
            pass

    def __del__(self):
        self.stop()

# vim:tabstop=4:expandtab