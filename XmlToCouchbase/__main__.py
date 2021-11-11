
#Fixme xmljson Dateien in virtual environment einbinden, damit es unabhängig von meinem pc funzt
# Environment ist angelegt aber wie genau nutzen?
import json
import sys
import os
import time
from os import path
import lxml.etree as ET
from json import dumps

from XmlToCouchbase.couchbase_handler import CouchbaseConnection

sys.path.insert(0,"C:/Users/Gsell/PycharmProjects/xmljson_fork")
import xmljson # ist unterstrichen funzt aber trotzdem // package heißt trotzdem xml json auch wenn es eine fork ist

import xmlschema

#todo directory als argument übergeben am besten, ggf. auch convention übergeben
#oder config datei für die directories anlegen
def xml_to_json():
    """convert XML from directory_in to JSON in directory_out """
    jsonStringList = []

    my_schema = xmlschema.XMLSchema("C:/Users/Gsell/PycharmProjects/xmljson/tests/MINiML.xsd")

    # directory with input xml
    directory_in_str = "C:/Users/Gsell/Documents/danio_rerio_benchmark"
    # output directory for json
    directory_out_str = "C:/Users/Gsell/Documents/danio_rerio_benchmark_json"

    parser = ET.XMLParser(remove_blank_text=True)
    with os.scandir(directory_in_str) as it:
        for entry in it:
            if entry.name.endswith(".xml") and entry.is_file():
                #parse xml file

                # print(entry.name + " " + str(my_schema.is_valid(entry.path)))
                # remove newlines from XML for clean JSON // parser can only remove blanks not new line
                # if problems with decoding set interpreter option -Xutf8
                # f = open(entry.path, "r")
                # string_without_line_breaks = ""
                # for line in f:
                #     stripped_line = line.rstrip()
                #     string_without_line_breaks += stripped_line
                # f.close()
                # f = open(entry.path, "w")
                # f.write(string_without_line_breaks)
                # f.close()

                #
                tree = ET.parse(entry.path, parser)
                root = tree.getroot()
                #convert xml to json

                converter = xmljson.GData(harmonize_synonyms=True)
                jsonObj = converter.data(root) #todo das ist alles das ich messen muss
                jsonString = dumps(jsonObj)
                jsonStringList.append(jsonString)

                name, extension = path.splitext(entry.name)
                # write json to file
                completeName = os.path.join(directory_out_str, name+".JSON")
                jsonFile = open(completeName, "w")
                jsonFile.write(jsonString)
                jsonFile.close()

def json_to_couchbase():
    """upload json from directory to couchbase"""

    # # directory with input xml
    directory_in_str = "C:/Users/Gsell/Documents/danio_rerio_benchmark_json"
    cb = CouchbaseConnection('couchbase://localhost:8091', 'admin', 'testpw')
    with os.scandir(directory_in_str) as it:
        for entry in it:
            if entry.name.endswith(".JSON") and entry.is_file():
                name, extension = os.path.splitext(entry.name)

                with open(entry) as json_file:
                    data = json.load(json_file)
                    cb.upsert_document(data, name)

def print_hi():
    #start_time = time.monotonic()
    tree = ET.parse('GSE138493_family.xml')
    root = tree.getroot()
    badgerfish_converter = xmljson.Abdera() #ns_as_attrib=False, ns_as_prefix=True)
    jsonObj = badgerfish_converter.data(root)

    print(dumps(jsonObj))
    #print('seconds: ', time.monotonic() - start_time)

if __name__ == '__main__':
    #print_hi()
    xml_to_json()

