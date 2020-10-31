from pprint import pprint
import json
import requests

from confluent_kafka import Producer

IEX_HIST_ENDPOINT = 'https://iextrading.com/api/1.0/hist'


def main():

    content = requests.get(IEX_HIST_ENDPOINT)

    content = json.loads(content.content.decode('utf-8'))

    for date_key, entries in content.items():
        #    assert len(entries) == 1
        entries = list(filter(lambda e: e['feed'] == 'TOPS', entries))

        content[date_key] = [{'date': '{}-{}-{}'.format(e['date'][:4], e['date'][4:6], e['date'][6:]),
                              'feed': e['feed'],
                              'link': e['link'],
                              'protocol': e['protocol'],
                              'download_size': e['size'],
                              'version': e['version']} for e in entries]

    content = {f'{k[:4]}-{k[4:6]}-{k[6:]}': v for k, v in content.items()}

    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    for date_key, entries in content.items():
        for idx, entry in enumerate(entries):
            key = f'{date_key}_{idx}'

            producer.produce('iex-available', json.dumps(entry).encode('utf-8'), key)

    producer.flush()


if __name__ == '__main__':
    main()
