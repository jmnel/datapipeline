from pymongo import MongoClient
from confluent_kafka import Consumer, Producer
from nameko.extensions import Entrypoint


class Updater(Entrypoint):

    name = 'service'

    def __init__(self):
        pass

    def setup(self):
        pass

    def start(self):
        self.container.spawn_managed_thread(self.run, identifier='Updater.run')

    def run(self):
        pass

    def stop(self):
        pass


#updater = Updater.decorator
