import unittest
from unittest import TestCase

import time
from multiprocessing import Process

import pika

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
        self.complex_avro_message.array_complex_field.add()
        self.complex_avro_message.array_complex_field[0].field_1 = "bbb"
        self.complex_avro_message.array_simple_field.add()
        self.complex_avro_message.array_simple_field[0] = "ccc"
        self.complex_avro_message.record_field.field_1 = "ddd"

        self.complex_avro_encoded = '\x020\x8e\xd1\x87\x01\x06aaa\x02\x06bbb\x00\x02\x06ccc\x00\x06ddd'

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

    def test_simple_message_value_assignment(self, m=None):
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
        self.test_simple_message_value_assignment(m)

        with self.assertRaises(ValueError):
            m.array_complex_field = [{"field_1": "aaa"}]

        with self.assertRaises(ValueError):
            m.record_field = {"field_1": "aaa"}

        m.array_complex_field.add()
        self.assertEqual(len(m.array_complex_field), 1)
        self.assertIsInstance(m.array_complex_field[0], _Item)
        m.array_complex_field[0].field_1 = "bbb"
        self.assertEqual(m.array_complex_field[0].field_1, "bbb")

        m.array_complex_field.add()
        self.assertEqual(len(m.array_complex_field), 2)
        self.assertIsInstance(m.array_complex_field[1], _Item)
        m.array_complex_field[1].field_1 = "ccc"
        self.assertEqual(m.array_complex_field[1].field_1, "ccc")

        m.array_simple_field.add()
        self.assertEqual(len(m.array_simple_field), 1)
        self.assertEqual(m.array_simple_field[0], None)
        m.array_simple_field[0] = "ccc"
        self.assertEqual(m.array_simple_field[0], "ccc")

        m.array_simple_field.add()
        self.assertEqual(len(m.array_simple_field), 2)
        self.assertEqual(m.array_simple_field[1], None)
        m.array_simple_field[1] = "ddd"
        self.assertEqual(m.array_simple_field[1], "ddd")

        del m.array_complex_field[1]
        self.assertEqual(len(m.array_complex_field), 1)

        del m.array_simple_field[1]
        self.assertEqual(len(m.array_simple_field), 1)

        m.record_field.field_1 = 'aaa'
        self.assertEqual(m.record_field.field_1, 'aaa')

    def test_set_content(self):
        content = {
            "id": 1111111,
            "name": "aaa",
            "array_complex_field": [{
                "field_1": "bbb"
            }],
            "array_simple_field": ["ccc"],
            "record_field": {
                "field_1": "ddd"
            }
        }

        # test for the entire message
        m = self.avro_factory.create("TEST_COMPLEX")
        m.set_content(content)

        self.assertEqual(m.id, 1111111)
        self.assertEqual(m.name, "aaa")
        self.assertEqual(len(m.array_complex_field), 1)
        self.assertEqual(m.array_complex_field[0].field_1, "bbb")
        self.assertEqual(len(m.array_simple_field), 1)
        self.assertEqual(m.array_simple_field[0], "ccc")
        self.assertEqual(m.record_field.field_1, "ddd")

        # test for _Item and _Array classes
        m = self.avro_factory.create("TEST_COMPLEX")

        m.array_complex_field.set_content(content["array_complex_field"])
        m.array_simple_field.set_content(content["array_simple_field"])
        m.record_field.set_content(content["record_field"])

        self.assertEqual(len(m.array_complex_field), 1)
        self.assertEqual(m.array_complex_field[0].field_1, "bbb")
        self.assertEqual(len(m.array_simple_field), 1)
        self.assertEqual(m.array_simple_field[0], "ccc")
        self.assertEqual(m.record_field.field_1, "ddd")

    def test_set_content_wrong(self):
        content = {
            "wrong_id": 1111111,
            "wrong_name": "aaa",
            "wrong_array_complex_field": [{
                "wrong_field_1": "bbb"
            }],
            "wrong_array_simple_field": ["ccc"],
            "wrong_record_field": {
                "wrong_field_1": "ddd"
            }
        }

        # test for the entire message
        m = self.avro_factory.create("TEST_COMPLEX")
        self.assertRaises(AttributeError, m.set_content, (content,))

        # test for _Item and _Array classes
        self.assertRaises(AttributeError, m.array_complex_field.set_content,
                          (content["wrong_array_complex_field"],))

        self.assertRaises(AttributeError, m.record_field.set_content,
                          (content["wrong_record_field"],))

    def test_retrieve(self):
        m = self.avro_factory.retrieve('\x020\x8e\xd1\x87\x01\x06aaa\x02\x06bbb\x00\x02\x06ccc\x00\x06ddd')
        self.assertEqual(m.id, 1111111)
        self.assertEqual(m.name, "aaa")
        self.assertEqual(len(m.array_complex_field), 1)
        self.assertEqual(m.array_complex_field[0].field_1, "bbb")
        self.assertEqual(len(m.array_simple_field), 1)
        self.assertEqual(m.array_simple_field[0], "ccc")
        self.assertEqual(m.record_field.field_1, "ddd")

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

    def test_amqp_receiver_errors(self):
        broker = AMQPReceiver()
        self.assertRaisesRegexp(AMQPError, "You must set the AMQP exchange", broker.run)
        broker.exchange = RABBIT_EXCHANGE
        self.assertRaisesRegexp(AMQPError, "You must configure the queue", broker.run)
        broker.set_queue(RABBIT_QUEUE, False, False)
        self.assertRaisesRegexp(AMQPError, "You must set the handler", broker.run)

    def test_amqp_broker_server_down(self):
        def handler(message_body, message_type):
            self.assertEqual(message_body, self.avro_encoded)
            self.assertEqual(message_type, self.avro_message.message_type)

        broker = AMQPReceiver('localhost', 20000)  # non existent rabbit server
        broker.handler = handler
        broker.set_queue(RABBIT_QUEUE, False, True)

        with self.assertRaises(AMQPError) as e:
            broker.exchange = RABBIT_EXCHANGE
            self.assertEqual(e.expected, "Cannot connect to AMQP server")
        with self.assertRaises(AMQPError) as e:
            broker.run()
            self.assertEqual(e.expected, "Cannot connect to AMQP server")

if __name__ == '__main__':
    unittest.main()