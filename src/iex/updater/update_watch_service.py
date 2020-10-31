from pprint import pprint
import json
from pymongo import MongoClient
# import requests
# import datetime as dt
# from operator import itemgetter

from nameko.rpc import rpc
from confluent_kafka import Producer
from nameko_kafka import consume
from nameko.extensions import Extension


class UpdateWatchService():

    name = "iex_update_watch_service"

    @consume("iex-available", group_id="iex-update-watch", bootstrap_servers="localhost:9092")
    def method(self, event):
        key = event.key.decode('utf-8')
        client = MongoClient("localhost", 27017)

        db = client.iex

        if query  := db.known.find_one({"key": key}) is None:
            values = json.loads(event.value.decode('utf-8'))
            values["state"] = "need_download"
            db.known.insert(values)

        client.close()


# class UpdateWatchPurge():

#    @rpc
#    def purge(self):
#        client = MongoClient("localhost", 27017)
#        db = client.iex
#        db.known.remove({})
#        client.close()
