import os
import pytest
import XmlToCouchbase.BaseXClient as BaseXClient

from XmlToCouchbase.couchbase_handler  import CouchbaseConnection
from XmlToCouchbase.basex_handler import BaseXConnection
cb = CouchbaseConnection('couchbase://138.201.66.27:8091', 'admin', 'testpw')
bx = BaseXConnection('localhost', 1984, 'admin', 'admin')


directory_in_str = "C:/Users/Gsell/Documents/danio_rerio_benchmark_json"
document_id_list = []
filename_list = []


# list with 300 tags
tag_list = cb.lookup_tags_benchmark()
tag_names = []
for i in tag_list: tag_names.append(i[0])

# list with 300 corresponding content
content_list = cb.lookup_content_benchmark()

tag_content_list = []
for i in range(300):
    tag_content_list.append((tag_list[i][0], content_list[i][0]))


numbers_list = []
for i in range(300): numbers_list.append(i)

# list with 300 sample ids
sample_id_list = cb.lookup_sample_ids_benchmark()
# list for naming results
sample_id_names = []
for i in sample_id_list: sample_id_names.append(i[0])


# list with 300 platform ids
platform_id_list = cb.lookup_platform_ids_benchmark()
#list used for naming results
platform_id_names = []
for i in platform_id_list: platform_id_names.append(i[0])


with os.scandir(directory_in_str) as it:
    for entry in it:
        if entry.name.endswith(".JSON") and entry.is_file():

            name, extension = os.path.splitext(entry.name)
            id, suffix = name.split("_")

            document_id_list.append([id])
            filename_list.append(id)



#todo brauche noch liste für alle parameter

# couchbase doc retrieval
def doc_retrieval_cb(id):
    cb.get_doc_by_key(id)
@pytest.mark.parametrize('id', document_id_list, ids=filename_list)
def test_doc_retrieval_cb(benchmark, id):
    benchmark.pedantic(doc_retrieval_cb, id, rounds=100, iterations=1)

# couchbase doc retrieval by selection - only use if times are different

def doc_retrieval_by_id_cb(id):
    cb.lookup_doc_by_id(id)
@pytest.mark.parametrize('id', document_id_list, ids=filename_list)
def test_doc_retrieval_by_id_cb(benchmark, id):
    benchmark.pedantic(doc_retrieval_by_id_cb, id, rounds=100, iterations=1)

# retrieve all sample ids and charateristics for given series id | selection+projection of deep data
def lookup_characteristic_by_id_cb(id):
     cb.lookup_characteristics(id)
@pytest.mark.parametrize('id', document_id_list, ids=filename_list)
def test_characteristic_by_id_cb(benchmark, id):
    benchmark.pedantic(lookup_characteristic_by_id_cb, id, rounds=100, iterations=1)


#
def lookup_sample_by_charateristic_cb(tag, content):
    cb.lookup_sample_by_characteristic(tag, content)
@pytest.mark.parametrize("tag, content", tag_content_list, ids=numbers_list)
def test_lookup_sample_by_characteristic_cb(benchmark, tag, content):
    benchmark.pedantic(lookup_sample_by_charateristic_cb, (tag,content), rounds=100, iterations=1)

# query attribute data
def lookup_sample_tags_cb(id):
    cb.lookup_sample_tags(id)
@pytest.mark.parametrize('id', sample_id_list, ids=sample_id_names)
def test_lookup_sample_tags_cb(benchmark, id):
    benchmark.pedantic(lookup_sample_tags_cb, id, rounds=100, iterations=1)

# attribute data in condition
# lookup data for documents with specified tag
def lookup_sample_id_by_tag_cb(tag):
    cb.lookup_sample_id_by_tag(tag)
@pytest.mark.parametrize('tag', tag_list, ids=tag_names)
def test_lookup_sample_id_by_tag_cb(benchmark, tag):
    benchmark.pedantic(lookup_sample_id_by_tag_cb, tag, rounds=100, iterations=1)


def lookup_series_by_platform_id_cb(id):
    cb.lookup_series_by_platform_id(id)
@pytest.mark.parametrize('id', platform_id_list, ids=platform_id_names)
def test_lookup_series_by_platform_id_cb(benchmark, id):
    benchmark.pedantic(lookup_series_by_platform_id_cb, id, rounds=100, iterations=1)


################################# BaseX Benchmark #####################################


# baseX doc retrieval by selection
def doc_retrieval_by_id_bx(id):
    bx.lookup_doc_by_id(id)
@pytest.mark.parametrize('id', document_id_list, ids=filename_list)
def test_doc_retrieval_by_id_bx(benchmark, id):
    benchmark.pedantic(doc_retrieval_by_id_bx, id, rounds=100, iterations=1)

#
def lookup_characteristic_by_id_bx(id):
     bx.lookup_characteristics(id)
@pytest.mark.parametrize('id', document_id_list, ids=filename_list)
def test_lookup_characteristic_by_id_bx(benchmark, id):
    benchmark.pedantic(lookup_characteristic_by_id_bx, id, rounds=100, iterations=1)


#
def lookup_sample_by_characteristic_bx(tag, content):
    bx.lookup_sample_by_characteristic(tag, content)
@pytest.mark.parametrize("tag, content", tag_content_list, ids=numbers_list)
def test_lookup_sample_by_characteristic_bx(benchmark, tag, content):
    benchmark.pedantic(lookup_sample_by_characteristic_bx, (tag,content), rounds=100, iterations=1)

# query attribute data
def lookup_sample_tags_bx(id):
    bx.lookup_sample_tags(id)
@pytest.mark.parametrize('id', sample_id_list, ids=sample_id_names)
def test_lookup_sample_tags_bx(benchmark, id):
    benchmark.pedantic(lookup_sample_tags_bx, id, rounds=100, iterations=1)

# attribute data in condition
# lookup data for documents with specified tag
def lookup_sample_id_by_tag_bx(tag):
    bx.lookup_sample_id_by_tag(tag)
@pytest.mark.parametrize('tag', tag_list, ids=tag_names)
def test_lookup_sample_id_by_tag_bx(benchmark, tag):
    benchmark.pedantic(lookup_sample_id_by_tag_bx, tag, rounds=100, iterations=1)


def lookup_series_by_platform_id_bx(id):
    bx.lookup_series_by_platform_id(id)
@pytest.mark.parametrize('id', platform_id_list, ids=platform_id_names)
def test_lookup_series_by_platform_id_bx(benchmark, id):
    benchmark.pedantic(lookup_series_by_platform_id_bx, id, rounds=100, iterations=1)