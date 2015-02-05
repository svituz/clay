#!/usr/bin/env python

import time

from clay.factory import MessageFactory
from clay.messenger import KafkaMessenger
from clay.serializer import AvroSerializer

from example_catalog import SINGLE_EXAMPLE_CATALOG

mf = MessageFactory(AvroSerializer, SINGLE_EXAMPLE_CATALOG)

# equivalent to KafkaMessenger(host='localhost', port=9092)
messenger = KafkaMessenger()

messenger.add_queue('EXAMPLES', durable=True, response=False)

# create the message of type 'DEPOSIT'
m = mf.create("DEPOSIT")

# populate the mandatory message fields
m.timestamp = str(time.time())
m.client_id = "John Doe"
m.atm_id = "Rome_101"
m.amount = 100

# send the message using the Messenger
messenger.send(m)

# vim: ts=4 et
