import unittest
from unittest import TestCase

import time
from multiprocessing import Process

import pika
from pika.exceptions import AMQPConnectionError

from clay.exceptions import InvalidMessage, SchemaException
from clay.factory import MessageFactory
from clay.serializer import AvroSerializer, AbstractHL7Serializer
from clay.messenger import AMQPMessenger, AMQPReceiver, AMQPError
from clay.message import _Item

from tests import TEST_CATALOG, TEST_SCHEMA, RABBIT_QUEUE, RABBIT_EXCHANGE


class TestMessage(TestCase):
    def setUp(self):
        self.avro_factory = MessageFactory(AvroSerializer, TEST_CATALOG)
        self.hl7_factory = MessageFactory(AbstractHL7Serializer, TEST_CATALOG)

        self.avro_message = self.avro_factory.create('TEST')
        self.avro_message.id = 1111111
        self.avro_message.name = "aaa"
        self.avro_encoded = '\x00\x10\x8e\xd1\x87\x01\x06aaa'

        self.complex_avro_message = self.avro_factory.create('TEST_COMPLEX')
        self.complex_avro_message.id = 1111111
        self.complex_avro_message.name = "aaa"
        self.complex_avro_message.items.add()
        self.complex_avro_message.items[0].name = "bbb"
        self.complex_avro_message.simple_items.add()
        self.complex_avro_message.simple_items[0] = "ccc"

        self.complex_avro_encoded = '\x02(\x8e\xd1\x87\x01\x06aaa\x02\x06bbb\x00\x02\x06ccc\x00'

    def tearDown(self):
        self._reset()

    def _reset(self):
        conn_param = pika.ConnectionParameters('localhost')
        connection = pika.BlockingConnection(conn_param)
        channel = connection.channel()
        channel.exchange_delete(RABBIT_EXCHANGE)
        channel.queue_delete(RABBIT_QUEUE)
        connection.close()
        pass

    def test_message_instantiation(self):
        test_message = self.avro_factory.create("TEST")
        self.assertEqual("TEST", test_message.message_type)
        self.assertEqual(TEST_SCHEMA, test_message.schema)

    def test_invalid_message_instantiation(self):
        self.assertRaises(InvalidMessage, self.avro_factory.create, "UNK")
        self.assertRaises(InvalidMessage, self.hl7_factory.create, "UNK")

    def test_message_value_assignment(self, m=None):
        if m is None:
            m = self.avro_factory.create("TEST")
        self.assertEqual(m.id, None)
        self.assertEqual(m.name, None)
        m.id = 1111111
        m.name = "aaa"

        self.assertEqual(m.id, 1111111)
        self.assertEqual(m.name, "aaa")

        # with self.assertRaises(AttributeError):
        #     m.unkn = 1

    def test_wrong_schema(self):
        with self.assertRaises(SchemaException):
            self.avro_factory.create("TEST_WRONG")

    def test_complex_message_value_assignment(self):
        m = self.avro_factory.create("TEST_COMPLEX")
        self.test_message_value_assignment(m)

        m.items.add()
        self.assertEqual(len(m.items), 1)
        self.assertIsInstance(m.items[0], _Item)
        m.items[0].name = "bbb"
        self.assertEqual(m.items[0].name, "bbb")

        m.items.add()
        self.assertEqual(len(m.items), 2)
        self.assertIsInstance(m.items[1], _Item)
        m.items[1].name = "ccc"
        self.assertEqual(m.items[1].name, "ccc")

        m.simple_items.add()
        self.assertEqual(len(m.simple_items), 1)
        self.assertEqual(m.simple_items[0], None)
        m.simple_items[0] = "ccc"
        self.assertEqual(m.simple_items[0], "ccc")

        m.simple_items.add()
        self.assertEqual(len(m.simple_items), 2)
        self.assertEqual(m.simple_items[1], None)
        m.simple_items[1] = "ddd"
        self.assertEqual(m.simple_items[1], "ddd")

        del m.items[1]
        self.assertEqual(len(m.items), 1)

        del m.simple_items[1]
        self.assertEqual(len(m.simple_items), 1)

    def test_avro_serializer(self):
        value = self.avro_message.serialize()
        self.assertEqual(value, self.avro_encoded)

        value = self.complex_avro_message.serialize()
        self.assertEqual(value, self.complex_avro_encoded)

    def test_amqp_transaction_no_response(self):
        def handler(message_body, message_type):
            self.assertEqual(message_body, self.avro_encoded)
            self.assertEqual(message_type, self.avro_message.message_type)

        broker = AMQPReceiver()
        broker.exchange = RABBIT_EXCHANGE
        broker.set_queue(RABBIT_QUEUE, False, False)
        broker.handler = handler

        p = Process(target=broker.run)
        p.start()

        time.sleep(1)

        messenger = AMQPMessenger()
        messenger.exchange = RABBIT_EXCHANGE
        messenger.add_queue(RABBIT_QUEUE, False, False)

        result = messenger.send(self.avro_message)
        self.assertEqual(result, None)
        p.terminate()
        p.join()

    def test_amqp_transaction_response(self):
        response = 'OK'

        def handler(message_body, message_type):
            self.assertEqual(message_body, self.avro_encoded)
            self.assertEqual(message_type, self.avro_message.message_type)
            return 'OK'

        broker = AMQPReceiver()
        broker.exchange = RABBIT_EXCHANGE
        broker.set_queue(RABBIT_QUEUE, False, True)
        broker.handler = handler

        p = Process(target=broker.run)
        p.start()

        time.sleep(1)

        messenger = AMQPMessenger()
        messenger.exchange = RABBIT_EXCHANGE
        messenger.add_queue(RABBIT_QUEUE, False, True)

        result = messenger.send(self.avro_message)
        self.assertEqual(result, response)
        p.terminate()
        p.join()

    def test_amqp_producer_server_down(self):
        messenger = AMQPMessenger('localhost', 20000)  # non existent rabbit server
        messenger.exchange = RABBIT_EXCHANGE
        messenger.add_queue(RABBIT_QUEUE, False, False)

        result = messenger.send(self.avro_message)
        self.assertIsNone(result)
        self.assertEqual(messenger._message_queue.qsize(), 1)

    def test_amqp_producer_non_existent_queue(self):
        self._reset()
        messenger = AMQPMessenger()
        messenger.exchange = RABBIT_EXCHANGE

        # the queue has not been specified yet
        with self.assertRaises(AMQPError):
            messenger.send(self.avro_message)

        messenger.add_queue(RABBIT_QUEUE, False, False)
        result = messenger.send(self.avro_message)
        self.assertIsNone(result)
        self.assertEqual(messenger._message_queue.qsize(), 1)

    def test_amqp_broker_server_down(self):
        def handler(message_body, message_type):
            self.assertEqual(message_body, self.avro_encoded)
            self.assertEqual(message_type, self.avro_message.message_type)

        broker = AMQPReceiver('localhost', 20000)  # non existent rabbit server
        broker.handler = handler
        broker.set_queue(RABBIT_QUEUE, False, True)

        with self.assertRaises(AMQPConnectionError):
            broker.exchange = RABBIT_EXCHANGE
        with self.assertRaises(AMQPConnectionError):
            broker.run()

if __name__ == '__main__':
    unittest.main()