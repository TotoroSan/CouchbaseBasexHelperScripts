import json
import os

from couchbase_handler import CouchbaseConnection

# directory with input xml
directory_in_str = ""
# initialize couchbase connection
cb = CouchbaseConnection('couchbase://localhost:8091', 'admin', 'admin')
with os.scandir(directory_in_str) as it:
    for entry in it:
        if entry.name.endswith(".JSON") and entry.is_file():
            name, extension = os.path.splitext(entry.name)
            id, suffix = name.split("_")
            with open(entry) as json_file:
                data = json.load(json_file)
                cb.upsert_document(data, id)

