import logging

logging.basicConfig()


class Messenger(object):
    def __init__(self):
        pass

    def send(self, message):
        pass


class Dummy(Messenger):
    def __init__(self):
        pass

    def send(self, serializer):
        print("Dummy using messenger", serializer.serialize())

# Imports the other Messengers
from .amqp_messenger import AMQPMessenger, AMQPReceiver
from .mqtt_messenger import MQTTMessenger, MQTTReceiver
from .kafka_messenger import KafkaMessenger, KafkaError

# vim:tabstop=4:expandtab
