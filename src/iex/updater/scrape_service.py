from pprint import pprint
import json
import requests
import datetime as dt
from operator import itemgetter
from pprint import pprint

from nameko.rpc import rpc
from confluent_kafka import Producer
# from nameko_kafka import KafkaProducer

IEX_HIST_ENDPOINT = "https://iextrading.com/api/1.0/hist"


def is_weekday(date_key: str):
    return dt.datetime.strptime(date_key, "%Y-%m-%d").weekday() < 5


def get_newest(entries):

    assert len(entries) == 1 or len(entries) == 2
    if len(entries) == 2:
        #        pprint(entries)
        if entries[0]["version"] == "1.6":
            return entries[0]
        else:
            #            print(entries[0]["date"])
            #            print("1->" + entries[0]["version"])
            #            print("2->" + entries[1]["version"])
            assert entries[1]["version"] == "1.6"
            return entries[1]
    else:
        return entries[0]


class ScrapeService:

    name = "iex_scrape_service"

    @rpc
    def scrape(self):
        print("running")
        content = requests.get(IEX_HIST_ENDPOINT)
        content = json.loads(content.content.decode("utf-8"))

        entries = [(f'{k[:4]}-{k[4:6]}-{k[6:]}', v) for k, v in content.items()]
        entries = [(k, list(filter(lambda e: e["feed"] == "TOPS", v))) for k, v in entries]
        entries = list(filter(lambda e: is_weekday(e[0]), entries))
        entries = list((e[0], get_newest(e[1])) for e in entries)
#        entries = list((e[0], {
        entries = [(k, {"date": "{}-{}-{}".format(e["date"][:4], e["date"][4:6], e["date"][6:]),
                        "feed": e["feed"],
                        "link": e["link"],
                        "protocol": e["protocol"],
                        "download_size": e["size"],
                        "version": e["version"]}) for k, e in entries]

        producer = Producer({"bootstrap.servers": "localhost:9092"})
        for date_key, entry in entries:
            producer.produce("iex-available", json.dumps(entry).encode("utf-8"), date_key)

        producer.flush()

#        for idx, e in entries:
#            print(f'{idx} : {e}')


#        for date_key, entries in content.items():
        #    assert len(entries) == 1
#            entries = list(filter(lambda e: e["feed"] == "TOPS", entries))

#            content[date_key] = [{"date": "{}-{}-{}".format(e["date"][:4], e["date"][4:6], e["date"][6:]),
#                                  "feed": e["feed"],
#                                  "link": e["link"],
#                                  "protocol": e["protocol"],
#                                  "download_size": e["size"],
#                                  "version": e["version"]} for e in entries]

#        content = {f"{k[:4]}-{k[4:6]}-{k[6:]}": v for k, v in content.items()}

#        producer = Producer({"bootstrap.servers": "localhost:9092"})

#        for date_key, entries in content.items():

#            if is_weekday(date_key)

#            for idx, entry in enumerate(entries):
#                key = f"{date_key}_{idx}"

#                producer.produce("iex-available", json.dumps(entry).encode("utf-8"), key)

#        producer.flush()

        return "Done"


# if __name__ == "__main__":
#    main()
