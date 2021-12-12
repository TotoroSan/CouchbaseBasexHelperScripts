import json
import os

from couchbase_handler import CouchbaseConnection

# directory with input xml
directory_in_str = "C:/Users/Gsell/Documents/danio_rerio_benchmark_json"
# initialize couchbase connection
cb = CouchbaseConnection('couchbase://138.201.66.27:8091', 'admin', 'testpw')
with os.scandir(directory_in_str) as it:
    for entry in it:
        if entry.name.endswith(".JSON") and entry.is_file():
            name, extension = os.path.splitext(entry.name)
            id, suffix = name.split("_")
            with open(entry) as json_file:
                data = json.load(json_file)
                cb.upsert_document(data, id)

