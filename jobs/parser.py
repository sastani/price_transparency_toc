import ijson
import requests
import gzip
import json


def parse_response(url, counter=1):
    if url.split('.')[-1] == 'gz':
        zipped = True
    else:
        zipped = False

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
                        yield obj, counter
                        builder = ijson.ObjectBuilder()
                        counter += 1
                    elif (prefix, event, value) == ('reporting_structure', 'end_array', None):
                        return

    # pre process JSON data coming in as stream from web request
def pre_process_data(spark, url, file_name, file_path, num_objs_in_file):

    json_payload = []
    obj_count_str = str()

    for obj, obj_count in parse_response(url):
        json_payload.append(obj)
        if obj_count % num_objs_in_file == 0:
            num_chunk = obj_count / num_objs_in_file
            obj_count_str = str(obj_count)
            obj_file_name = file_name + "-" + obj_count_str
            obj_file_path = file_path + obj_file_name + '.json'
            f = open(obj_file_path, 'w')
            json.dump(json_payload, fp=f, separators=(',', ':'))
            yield num_chunk, obj_file_path
            json_payload = []

    # create file for any leftover objects
    if obj_count < num_objs_in_file:
        obj_file_name = file_name + '-0'
    else:
        obj_file_name = file_name + "-leftover"
    obj_file_path = file_path + obj_file_name
    f = open(obj_file_path + '.json', 'w')
    json.dump(json_payload, fp=f, separators=(',', ':'))
    yield obj_count, obj_file_path

