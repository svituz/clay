import unittest
from unittest import TestCase

import time
from multiprocessing import Process

from clay.factory import MessageFactory
from clay.serializer import AvroSerializer
from clay.messenger import MQTTMessenger, MQTTReceiver, MQTTError

from tests import TEST_CATALOG, RABBIT_QUEUE


class TestMessage(TestCase):
    def setUp(self):
        self.avro_factory = MessageFactory(AvroSerializer, TEST_CATALOG)

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
        pass

    def test_mqtt_transaction_no_response(self):
        def handler(message_body, message_type):
            self.assertEqual(message_body, self.avro_encoded)
            self.assertEqual(message_type, self.avro_message.message_type)

        broker = MQTTReceiver()
        broker.set_queue(RABBIT_QUEUE, False, False)
        broker.handler = handler

        p = Process(target=broker.run)
        p.start()

        time.sleep(1)

        messenger = MQTTMessenger()
        messenger.add_queue(RABBIT_QUEUE, False, False)

        result = messenger.send(self.avro_message)
        self.assertEqual(result, None)
        p.terminate()
        p.join()

    def test_mqtt_producer_server_down(self):
        messenger = MQTTMessenger('localhost', 20000)  # non existent rabbit server
        messenger.add_queue(RABBIT_QUEUE, False, False)

        result = messenger.send(self.avro_message)
        self.assertIsNone(result)
        self.assertEqual(messenger._spooling_queue.qsize(), 1)

    def test_mqtt_producer_non_existent_queue(self):
        self._reset()
        messenger = MQTTMessenger()

        # the queue has not been specified yet
        with self.assertRaises(MQTTError):
            messenger.send(self.avro_message)

        messenger.add_queue(RABBIT_QUEUE, False, False)
        result = messenger.send(self.avro_message)

        self.assertIsNone(result)
        self.assertEqual(messenger._spooling_queue.qsize(), 0)

    def test_mqtt_broker_server_down(self):
        def handler(message_body, message_type):
            self.assertEqual(message_body, self.avro_encoded)
            self.assertEqual(message_type, self.avro_message.message_type)

        broker = MQTTReceiver('localhost', 20000)  # non existent rabbit server
        broker.handler = handler
        broker.set_queue(RABBIT_QUEUE, False, True)

        with self.assertRaises(MQTTError):
            broker.run()

if __name__ == '__main__':
    unittest.main()