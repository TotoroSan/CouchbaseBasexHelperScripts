
import json
import sys
import os
import time
from os import path
import lxml.etree as ET
from json import dumps

from XmlToCouchbase.couchbase_handler import CouchbaseConnection

sys.path.insert(0,"insert path to xmljson_fork here")
import xmljson # package is stored locally and refers to xmljson_fork

import xmlschema


def xml_to_json():
    """convert XML from directory_in to JSON in directory_out """
    jsonStringList = []

    my_schema = xmlschema.XMLSchema("insert path to schema here")

    # directory with input xml
    directory_in_str = ""
    # output directory for json
    directory_out_str = ""

    parser = ET.XMLParser(remove_blank_text=True)
    with os.scandir(directory_in_str) as it:
        for entry in it:
            if entry.name.endswith(".xml") and entry.is_file():
                #parse xml file

                #print(entry.name + " " + str(my_schema.is_valid(entry.path)))
                #remove newlines from XML for clean JSON // parser can only remove blanks not new line
                #if problems with decoding set interpreter option -Xutf8
                f = open(entry.path, "r")
                string_without_line_breaks = ""
                for line in f:
                    stripped_line = line.rstrip()
                    string_without_line_breaks += stripped_line
                f.close()
                f = open(entry.path, "w")
                f.write(string_without_line_breaks)
                f.close()

                #
                tree = ET.parse(entry.path, parser)
                root = tree.getroot()
                #convert xml to json

                converter = xmljson.GData(harmonize_synonyms=True)
                jsonObj = converter.data(root)
                jsonString = dumps(jsonObj)
                jsonStringList.append(jsonString)

                name, extension = path.splitext(entry.name)
                # write json to file
                completeName = os.path.join(directory_out_str, name+".JSON")
                jsonFile = open(completeName, "w")
                jsonFile.write(jsonString)
                jsonFile.close()


if __name__ == '__main__':
    xml_to_json()

