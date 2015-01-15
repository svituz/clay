#!/usr/bin/env python

from clay.factory import MessageFactory
from clay.messenger import AMQPBroker
from clay.serializer import AvroSerializer

from example_catalog import SINGLE_EXAMPLE_CATALOG

mf = MessageFactory(AvroSerializer, SINGLE_EXAMPLE_CATALOG)

def my_handler(body, message_type):
    try:
        print mf.retrieve(body).fields
    except Exception as ex:
        print ex

brk = AMQPBroker()
brk.exchange = 'EXAMPLES'
brk.set_queue('EXAMPLES', durable=True, response=False)
brk.handler = my_handler
brk.run()
