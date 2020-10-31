import json

from pymongo import MongoClient
from confluent_kafka import Producer


def main():

    client = MongoClient()
    db = client.iex

    stored_dates = db.stored_dates

    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    for obj in stored_dates.find():

        assert 'date' in obj
        obj = {'date': obj['date'], 'state': 'missing'}
        producer.produce('iex-stored', json.dumps(obj).encode('utf-8'), obj['date'])

    client.close()

    producer.flush()


if __name__ == '__main__':
    main()
