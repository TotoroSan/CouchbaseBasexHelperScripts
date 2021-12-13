import os
import time
import sys
from json import dumps

import pytest
from lxml import etree as ET
sys.path.insert(0,"reference path to xmljson_fork here")
import xmljson # ist unterstrichen funzt aber trotzdem // package heißt trotzdem xml json auch wenn es eine fork ist

# directory with input xml
directory_in_str = ""
# output directory for json
directory_out_str = ""

# contains root elements each in a list (because arguments need to be passed to benchmark as a list)
xml_tree_list = []
xml_filename_list = []
# tree = ET.parse('GSE169013_family.xml')
# root = tree.getroot()
# xml_file_list.append(root)

with os.scandir(directory_in_str) as it:
    for entry in it:
        if entry.name.endswith(".xml") and entry.is_file():
            parser = ET.XMLParser(remove_blank_text=True)
            tree = ET.parse(entry.path, parser)
            root = tree.getroot()
            # root needs to be added as list, since benchmark needs positional arguments for conversion as list
            xml_tree_list.append([root]) # add root to tree list
            xml_filename_list.append(entry.name)



# Testfunctions benchmark every conversion convention with pytest-benchmark. Needs to be called via CLI: python -m pytest in curr. dir. (executes all tests)
# specific tests
def badgerfish_conversion(tree):
    """
    Function that is benchmarked. Converts xml-Tree to JsonObject.
    """
    # # convert xml to json
    badgerfish_converter = xmljson.BadgerFish()#xml_schema="C:/Users/Gsell/PycharmProjects/xmljson/tests/MINiML.xsd") #ns_as_attrib=False, ns_as_prefix=True)
    jsonObj = badgerfish_converter.data(tree)

    #return dumps(jsonObj)
# parametrize is a decorator that calls the inner test case once per input in xml_tree_list.
@pytest.mark.parametrize('tree', xml_tree_list, ids=xml_filename_list)
def test_conversion_badgerfish(benchmark, tree):
    # benchmark the conversion
    # the positional arguments that are passed, in this case file, need to be a list or tuple.
    # rounds is how often the benchmark is repeated and iterations is how often a function call is repeated within a round
    # stats are computed over the results of the rounds
    benchmark.pedantic(badgerfish_conversion, tree, rounds=100, iterations=1)


def gdata_conversion(tree):
    gdata_converter = xmljson.GData()#xml_schema="C:/Users/Gsell/PycharmProjects/xmljson/tests/MINiML.xsd") #ns_as_attrib=False, ns_as_prefix=True)
    jsonObj = gdata_converter.data(tree)
@pytest.mark.parametrize('tree', xml_tree_list, ids=xml_filename_list)
def test_conversion_gdata(benchmark, tree):
    benchmark.pedantic(gdata_conversion, tree, rounds=100, iterations=1)


def parker_conversion(tree):
    parker_converter = xmljson.Parker(xml_schema="C:/Users/Gsell/PycharmProjects/xmljson/tests/MINiML.xsd") #ns_as_attrib=False, ns_as_prefix=True)
    jsonObj = parker_converter.data(tree)
@pytest.mark.parametrize('tree', xml_tree_list, ids=xml_filename_list)
def test_conversion_parker(benchmark, tree):
    benchmark.pedantic(parker_conversion, tree, rounds=100, iterations=1)


def abdera_conversion(tree):
    abdera_converter = xmljson.Abdera(xml_schema="C:/Users/Gsell/PycharmProjects/xmljson/tests/MINiML.xsd") #ns_as_attrib=False, ns_as_prefix=True)
    jsonObj = abdera_converter.data(tree)
@pytest.mark.parametrize('tree', xml_tree_list, ids=xml_filename_list)
def test_conversion_abdera(benchmark, tree):
    benchmark.pedantic(abdera_conversion, tree, rounds=100, iterations=1)


def yahoo_conversion(tree):
    yahoo_converter = xmljson.Yahoo(xml_schema="C:/Users/Gsell/PycharmProjects/xmljson/tests/MINiML.xsd") #ns_as_attrib=False, ns_as_prefix=True)
    jsonObj = yahoo_converter.data(tree)
@pytest.mark.parametrize('tree', xml_tree_list, ids=xml_filename_list)
def test_conversion_yahoo(benchmark, tree):
    benchmark.pedantic(yahoo_conversion, tree, rounds=100, iterations=1)



















