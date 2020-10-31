from pymongo import MongoClient
from confluent_kafka import Consumer, Producer


def main():

    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    consumer = Consumer({'bootstrap.servers': 'localhost:9092'})
