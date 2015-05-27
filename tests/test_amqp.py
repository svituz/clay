# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2015, CRS4
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import time
from multiprocessing import Process
from unittest import TestCase

import pika

from clay.messenger import AMQPMessenger, AMQPReceiver
from clay.factory import MessageFactory
from clay.serializer import AvroSerializer, AbstractHL7Serializer
from clay.exceptions import MessengerErrorConnectionRefused, MessengerErrorNoApplicationName, \
    MessengerErrorNoHandler, MessengerErrorNoQueue

from tests import TEST_CATALOG, RABBIT_QUEUE, RABBIT_EXCHANGE


class TestAMQP(TestCase):

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
        self.complex_avro_message.record_field.field_2 = "eee"

        self.complex_avro_encoded = '\x02@\x8e\xd1\x87\x01\x06aaa\x00\x02\x06bbb' \
                                    '\x00\x00\x02\x06ccc\x00\x00\x06ddd\x00\x06eee'

    def tearDown(self):
        self._reset()

    def _reset(self):
        conn_param = pika.ConnectionParameters('localhost')
        connection = pika.BlockingConnection(conn_param)
        channel = connection.channel()
        channel.exchange_delete(RABBIT_EXCHANGE)
        channel.queue_delete(RABBIT_QUEUE)
        connection.close()

    def test_amqp_transaction_no_response(self):
        def handler(message_body, message_type):
            self.assertEqual(message_body, self.avro_encoded)
            self.assertEqual(message_type, self.avro_message.message_type)

        broker = AMQPReceiver()
        broker.application_name = RABBIT_EXCHANGE
        broker.set_queue(RABBIT_QUEUE, False, False)
        broker.handler = handler

        p = Process(target=broker.run)
        p.start()

        time.sleep(1)

        messenger = AMQPMessenger()
        messenger.application_name = RABBIT_EXCHANGE
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
        broker.application_name = RABBIT_EXCHANGE
        broker.set_queue(RABBIT_QUEUE, False, True)
        broker.handler = handler

        p = Process(target=broker.run)
        p.start()

        time.sleep(1)

        messenger = AMQPMessenger()
        messenger.application_name = RABBIT_EXCHANGE
        messenger.add_queue(RABBIT_QUEUE, False, True)

        result = messenger.send(self.avro_message)
        self.assertEqual(result, response)
        p.terminate()
        p.join()

    def test_amqp_producer_server_down(self):
        messenger = AMQPMessenger('localhost', 20000)  # non existent rabbit server
        messenger.application_name = RABBIT_EXCHANGE
        messenger.add_queue(RABBIT_QUEUE, False, False)

        result = messenger.send(self.avro_message)
        self.assertIsNone(result)
        self.assertEqual(messenger._message_queue.qsize(), 1)

    def test_amqp_producer_non_existent_queue(self):
        self._reset()
        messenger = AMQPMessenger()
        messenger.application_name = RABBIT_EXCHANGE

        # the queue has not been specified yet
        with self.assertRaises(MessengerErrorNoQueue):
            messenger.send(self.avro_message)

        messenger.add_queue(RABBIT_QUEUE, False, False)
        result = messenger.send(self.avro_message)
        self.assertIsNone(result)
        self.assertEqual(messenger._message_queue.qsize(), 1)

    def test_amqp_receiver_errors(self):
        broker = AMQPReceiver()
        self.assertRaises(MessengerErrorNoApplicationName, broker.run)
        broker.application_name = RABBIT_EXCHANGE
        self.assertRaises(MessengerErrorNoQueue, broker.run)
        broker.set_queue(RABBIT_QUEUE, False, False)
        self.assertRaises(MessengerErrorNoHandler, broker.run)

    def test_amqp_broker_server_down(self):
        def handler(message_body, message_type):
            self.assertEqual(message_body, self.avro_encoded)
            self.assertEqual(message_type, self.avro_message.message_type)

        broker = AMQPReceiver('localhost', 20000)  # non existent rabbit server
        broker.handler = handler
        broker.set_queue(RABBIT_QUEUE, False, True)

        with self.assertRaises(MessengerErrorConnectionRefused) as e:
            broker.application_name = RABBIT_EXCHANGE
        with self.assertRaises(MessengerErrorConnectionRefused) as e:
            broker.run()
