import ijson
import requests
import gzip
from confluent_kafka import Producer
import json


def parse_response(url, counter=1):
    if url.split('.')[-1] == 'gz':
        zipped = True
    else:
        zipped = False
    p = Producer({'bootstrap.servers': '10.0.0.3:9093'})

    with requests.get(url, stream=True) as r:
        if zipped:
            f = gzip.GzipFile(fileobj=r.raw)
        else:
            f = r.raw

        parser = ijson.parse(f, use_float=True)
        for prefix, event, value in parser:
            # start creating objects if we have found the start of the array of reporting plan objects
            if (prefix, event, value) == ("reporting_structure", "start_array", None):
                builder = ijson.ObjectBuilder()
                # build each reporting plan object, mapping plan(s) to network file(s)
                for prefix, event, value in parser:
                    builder.event(event, value)
                    if (prefix, event) == ('reporting_structure.item', 'end_map'):
                        obj = builder.value
                        p.produce('json_events', json.dumps(obj))
                        yield counter
                        builder = ijson.ObjectBuilder()
                        counter += 1
                    elif (prefix, event, value) == ('reporting_structure', 'end_array', None):
                        return

