# needed for any cluster connection
import os

from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator

# needed to support SQL++ (N1QL) query
from couchbase.cluster import QueryOptions
import couchbase

import time
import timeit

class CouchbaseConnection(object):

    def __init__(self, server, username, password):
        """initialize a Couchbase Connection"""
        self.username = username
        self.password = password
        self.server = server

        self.cluster = Cluster(self.server, ClusterOptions(
            PasswordAuthenticator(self.username, self.password)))

        # connect to the server
        self.connect('test-data')

    def connect(self, bucket):
        """Connect to Coucbase bucket. Multiple simultaneous connections are possible"""

        self.cb_bucket = self.cluster.bucket(bucket)
        self.cb_coll = self.cb_bucket.default_collection()

        # You can access multiple buckets using the same Cluster object.
        #another_bucket = cluster.bucket("beer-sample")


    def upsert_document(self, doc, key):
        print("\nUpsert CAS: ")
        try:
            # key will equal: "airline_8091"
            #key = doc["type"] + "_" + str(doc["id"])
            result = self.cb_coll.upsert(key, doc)
            print(result.cas)
        except Exception as e:
            print(e)

    #part of benchmark
    def get_doc_by_key(self, key):
        """Retrieve document by key"""
        print("\nGet Result: ")
        try:
            result = self.cb_coll.get(key)
          #  print(result.content_as[str])

            print("Report execution time: {}".format(result.metadata().metrics().execution_time()))
        except Exception as e:
            print(e)

    #part of benchmark
    def lookup_doc_by_id(self, id):
        """Retrieve document by key, but via query (just for speed test purposes)"""
        print("\nLookup Result: ")
        try:
            #time_start = time.perf_counter_ns()
            sql_query = 'SELECT * FROM `test-data` WHERE META(`test-data`).id =$1'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[id], metrics=True))

            for row in row_iter: print(row)
            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))
        except Exception as e:
            print(e)

    def lookup_series_by_sample_id(self, sample_id):
        """lookup all series ids for documents that contain given sample id"""
        print("\nLookup Result: ")
        try:
            sql_query = 'SELECT META(`test-data`).id FROM `test-data` WHERE ANY s in MINiML.Sample SATISFIES s.iid = $1 END;'
            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[sample_id], metrics=True))

            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))

            #for row in row_iter: print(row)
        except Exception as e:
            print(e)

    def lookup_sample_ids(self, id):
        """lookup all sample ids contained ia document"""

        print("\nLookup Result: ")
        sample_id_list = []
        try:
            sql_query = 'SELECT custom_data.iid FROM (SELECT * FROM `test-data` UNNEST MINiML.Sample AS custom_data) ' \
                        'as outer_unnest WHERE META(`test-data`).id = $1'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[id], metrics=True))

            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))
            #for row in row_iter:
                #print(row)
                #sample_id_list.append([row["iid"]])

            #print(sample_id_list)
            #return sample_id_list
        except Exception as e:
            print(e)

    def lookup_sample_by_id(self, sample_id):
        """lookup sample data by sample id"""
        print("\nLookup Result: ")
        try:
            sql_query = 'SELECT custom_data FROM (SELECT * FROM `test-data` UNNEST MINiML.Sample AS custom_data) ' \
                        'AS outer_unnest WHERE outer_unnest.custom_data.iid = $1'
            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[sample_id], metrics=True))
            #for row in row_iter: print(row)
            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))
        except Exception as e:
            print(e)





    # part of benchmarking
    def lookup_characteristics(self, id):
        """lookup characteristics data of all samples for given series ID"""
        print("\nLookup Result: ")
        try:
            sql_query = 'SELECT custom_data.iid, custom_data.Channel.Characteristics FROM `test-data` UNNEST MINiML.Sample as custom_data  WHERE META(`test-data`).id = $1'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[id], metrics=True))
            #for row in row_iter: print(row)
            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))
        except Exception as e:
            print(e)

    def lookup_characteristics_alternative(self, id):
        """lookup characteristics data of all samples for given series ID"""
        print("\nLookup Result: ")
        try:
            sql_query = 'SELECT custom_data.iid, custom_data.Channel.Characteristics FROM `test-data` USE KEYS [$1] UNNEST MINiML.Sample as custom_data '

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[id], metrics=True))
            #for row in row_iter: print(row)
            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))
        except Exception as e:
            print(e)



    #benchmark this
    def lookup_sample_by_characteristic(self, tag, content):
        """lookup sample ids for which passed tag has passed content """

        # implement document retrieval per get (direct retrieval isn't possible because unnest inflates the result!)
        print("\nLookup Result: ")
        try:
            sql_query = 'SELECT outer_unnest.custom_data.iid ' \
                        'FROM (SELECT * FROM `test-data` UNNEST MINiML.Sample AS custom_data) as outer_unnest ' \
                        'UNNEST outer_unnest.custom_data.Channel.Characteristics as inner_unnest ' \
                        'WHERE inner_unnest.tag=$1 AND inner_unnest.`$t`=$2'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[tag, content]), metrics=True)
            #for row in row_iter: print(row)
            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))
        except Exception as e:
            print(e)

    def lookup_sample_by_characteristic_alternative(self, tag, content):
        """lookup sample ids for which passed tag has passed content """

        # implement document retrieval per get (direct retrieval isn't possible because unnest inflates the result!)
        print("\nLookup Result: ")
        try:
            sql_query = 'SELECT s.iid FROM `test-data` AS t UNNEST t.MINiML.Sample AS s UNNEST s.Channel.Characteristics as c WHERE c.tag=$1 AND c.`$t`=$2'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[tag, content]), metrics=True)
            #for row in row_iter: print(row)
            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))
        except Exception as e:
            print(e)

    #benchmark this (query attribute data)
    def lookup_sample_tags(self, sample_id):
        """lookup the characteristic tags for given sample ID"""
        print("\nLookup Result: ")
        try:
            sql_query = 'SELECT inner_unnest.tag FROM (SELECT * FROM `test-data` UNNEST MINiML.Sample AS custom_data WHERE custom_data.iid = $1) as outer_unnest UNNEST outer_unnest.custom_data.Channel.Characteristics as inner_unnest'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[sample_id]), metrics=True)
            for row in row_iter: print(row)
            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))
        except Exception as e:
            print(e)

    def lookup_sample_tags_alternative(self, sample_id):
        """lookup the characteristic tags for given sample ID"""
        print("\nLookup Result: ")
        try:
            sql_query = 'SELECT c.tag FROM `test-data` AS t UNNEST t.MINiML.Sample AS s UNNEST s.Channel.Characteristics AS c WHERE s.iid = $1'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[sample_id]), metrics=True)
            #for row in row_iter: print(row)
            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))
        except Exception as e:
            print(e)


    def lookup_sample_tag_content(self, sample_id, tag):
        """lookup content of a specific tag for a specific sample"""
        print("\nLookup Result: ")
        # note: ` ` around $t is escape character
        try:
            sql_query = 'SELECT inner_unnest.`$t` FROM (SELECT * FROM `test-data` UNNEST MINiML.Sample AS custom_data WHERE custom_data.iid = $1) as outer_unnest UNNEST outer_unnest.custom_data.Channel.Characteristics as inner_unnest WHERE inner_unnest.tag = $2'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[sample_id, tag]), metrics=True)

            #for row in row_iter:
                #print(row["$t"])
                #result = row["$t"] #only one result line, so it won't be overwritten
            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))
            #return result
        except Exception as e:
            print(e)

    def lookup_sample_id_by_tag(self, tag):
        """lookup ids of samples that have specified tag"""
        print("\nLookup Result: ")
        # note: ` ` around $t is escape character
        try:
            sql_query = 'SELECT outer_unnest.custom_data.iid ' \
                        'FROM (SELECT * FROM `test-data` UNNEST MINiML.Sample AS custom_data) as outer_unnest ' \
                        'UNNEST outer_unnest.custom_data.Channel.Characteristics as inner_unnest ' \
                        'WHERE inner_unnest.tag=$1'
            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[tag]), metrics=True)
            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))
            # for row in row_iter:
            #     print(row)

        except Exception as e:
            print(e)


    def lookup_sample_id_by_tag_alternative(self, tag):
        """lookup ids of samples that have specified tag"""
        print("\nLookup Result: ")
        # note: ` ` around $t is escape character
        try:
            sql_query = 'SELECT s.iid FROM `test-data` AS t UNNEST t.MINiML.Sample AS s UNNEST s.Channel.Characteristics as c WHERE c.tag=$1'
            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[tag]), metrics=True)
            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))
            # for row in row_iter:
            #     print(row)
        except Exception as e:
            print(e)

    def lookup_sample_id_by_tag_optimal(self, tag):
        """lookup ids of samples that have specified tag"""
        print("\nLookup Result: ")
        # note: ` ` around $t is escape character
        try:
            sql_query = 'SELECT fltr[1] AS iid FROM `test-data` AS t UNNEST t.MINiML.Sample AS s UNNEST s.Channel.Characteristics as c LET fltr = [`c`.`tag`, s.iid] WHERE fltr >= [$1] AND fltr < [SUCCESSOR($1)]'
            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[tag]), metrics=True)
            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))
            # for row in row_iter:
            #     print(row)
        except Exception as e:
            print(e)

    def lookup_all_tags_and_content(self):
        """lookup tag and content for ALL samples of ALL series"""

        print("\nLookup Result: ")
        try:
            sql_query = 'SELECT inner_unnest.tag, inner_unnest.`$t`, outer_unnest.custom_data.iid ' \
                        'FROM (SELECT * FROM `test-data` UNNEST MINiML.Sample AS custom_data) as outer_unnest ' \
                        'UNNEST outer_unnest.custom_data.Channel.Characteristics as inner_unnest'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[]), metrics=True)
            #for row in row_iter: print(row)
            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))
        except Exception as e:
            print(e)

    def lookup_series_by_platform_id(self, platform_id):
        """lookup all series ids for documents that use the specified platform"""
        print("\nLookup Result: ")
        try:
            sql_query = 'SELECT META(`test-data`).id FROM `test-data` WHERE MINiML.Platform.iid = $1;'
            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[platform_id]), metrics=True)
            #for row in row_iter: print(row)
            print("Report execution time: {}".format(row_iter.metadata().metrics().execution_time()))
        except Exception as e:
            print(e)


    ################ BENCHMARK HELPER FUNCTIONS ############################
    def lookup_tags_benchmark(self):
        """lookup tag and content for ALL samples of ALL series"""
        tag_list = []
        counter = 0
        print("\nLookup Result: ")
        try:
            sql_query = 'SELECT inner_unnest.tag ' \
                        'FROM (SELECT * FROM `test-data` UNNEST MINiML.Sample AS custom_data) as outer_unnest ' \
                        'UNNEST outer_unnest.custom_data.Channel.Characteristics as inner_unnest'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[]))
            for row in row_iter:
                if counter < 300:
                    tag_list.append([row['tag']])
                    counter += 1
            print(tag_list)
            return tag_list

        except Exception as e:
            print(e)

    def lookup_content_benchmark(self):
        """helper function for benchmark"""
        content_list = []
        counter = 0
        print("\nLookup Result: ")
        try:
            sql_query = 'SELECT inner_unnest.`$t` ' \
                        'FROM (SELECT * FROM `test-data` UNNEST MINiML.Sample AS custom_data) as outer_unnest ' \
                        'UNNEST outer_unnest.custom_data.Channel.Characteristics as inner_unnest'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[]))
            for row in row_iter:
                if counter < 300:
                    content_list.append([row['$t']])
                    counter += 1
            print (content_list)
            return content_list
        except Exception as e:
            print(e)


    def lookup_sample_ids_benchmark(self):
        """helper function for benchmar"""

        #print("\nLookup Result: ")
        sample_id_list = []
        try:
            sql_query = 'SELECT custom_data.iid FROM (SELECT * FROM `test-data` UNNEST MINiML.Sample AS custom_data) ' \
                        'as outer_unnest'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[]))
            for row in row_iter:
                #print(row)
                if len(sample_id_list) < 300:
                    sample_id_list.append([row["iid"]]) # todo remove, only temporary for benchmarking

            #print(sample_id_list)
            return sample_id_list
        except Exception as e:
            print(e)

    def lookup_platform_ids_benchmark(self):
        """helper function for benchmark"""

        #print("\nLookup Result: ")
        platform_id_list = []
        while len(platform_id_list) < 300:
            try:
                sql_query = 'SELECT MINiML.Platform.iid FROM `test-data` WHERE MINiML.Platform.iid IS NOT NULL'

                row_iter = self.cluster.query(
                    sql_query, QueryOptions(positional_parameters=[]))
                for row in row_iter:
                    #print(row)
                    if len(platform_id_list) < 300:
                        platform_id_list.append([row["iid"]]) # todo remove, only temporary for benchmarking

                #print(len(platform_id_list))

            except Exception as e:
                print(e)
        return platform_id_list


######################################harmonized lookups##################################

    def lookup_sample_tag_content_harmonized(self, sample_id, tag):
        """lookup content of a specific tag for a specific sample"""
        print("\nLookup Result: ")
        # note: ` ` around $t is escape character
        results = []
        try:
            sql_query = 'SELECT inner_unnest.raw_data.`$t` FROM (SELECT * FROM `test-data` UNNEST MINiML.Sample AS custom_data WHERE custom_data.iid = $1) as outer_unnest UNNEST outer_unnest.custom_data.Channel.Characteristics as inner_unnest WHERE inner_unnest.raw_data.tag = $2'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[sample_id, tag]))

            for row in row_iter:
                #print(row["$t"])
                result = (row["$t"]) #only one result line, so it won't be overwritten

            return result
        except Exception as e:
            print(e)

    def lookup_exposure_start_harmonized(self, sample_id):
        """lookup the exposure start for given sample_id"""
        print("\nLookup Result: ")
        # note: ` ` around $t is escape character
        results = []
        try:
            sql_query = 'SELECT custom_data.Channel.`Treatment-Protocol`.raw_data.exposure_start.`$t` FROM (SELECT * FROM `test-data` UNNEST MINiML.Sample AS custom_data WHERE custom_data.iid = $1) as outer_unnest'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[sample_id]))

            for row in row_iter:
                # print(row["$t"])
                result = (row["$t"])  # only one result line, so it won't be overwritten

            return result
        except Exception as e:
            print(e)

    def lookup_exposure_duration_harmonized(self, sample_id):
        """lookup the exposure start for given sample_id"""
        print("\nLookup Result: ")
        # note: ` ` around $t is escape character
        results = []
        try:
            sql_query = 'SELECT custom_data.Channel.`Treatment-Protocol`.raw_data.exposure_duration.`$t` FROM (SELECT * FROM `test-data` UNNEST MINiML.Sample AS custom_data WHERE custom_data.iid = $1) as outer_unnest'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[sample_id]))

            for row in row_iter:
                # print(row["$t"])
                result = (row["$t"])  # only one result line, so it won't be overwritten

            return result
        except Exception as e:
            print(e)

    def lookup_sample_concentration_harmonized(self, sample_id):
        """lookup content of a specific tag for a specific sample"""
        print("\nLookup Result: ")
        # note: ` ` around $t is escape character
        results = []
        try:
            sql_query = 'SELECT inner_unnest.raw_data.concentration.`$t` FROM (SELECT * FROM `test-data` UNNEST MINiML.Sample AS custom_data WHERE custom_data.iid = $1) as outer_unnest UNNEST outer_unnest.custom_data.Channel.Characteristics as inner_unnest WHERE inner_unnest.raw_data.concentration.`$t` IS NOT NULL'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[sample_id]))

            for row in row_iter:
                #print(row["$t"])
                result = (row["$t"]) #only one result line, so it won't be overwritten

            return result
        except Exception as e:
            print(e)

    def lookup_sample_compound_harmonized(self, sample_id):
        """lookup content of a specific tag for a specific sample"""
        print("\nLookup Result: ")
        # note: ` ` around $t is escape character
        results = []
        try:
            sql_query = 'SELECT inner_unnest.raw_data.compound.`$t` FROM (SELECT * FROM `test-data` UNNEST MINiML.Sample AS custom_data WHERE custom_data.iid = $1) as outer_unnest UNNEST outer_unnest.custom_data.Channel.Characteristics as inner_unnest WHERE inner_unnest.raw_data.compound.`$t` IS NOT MISSING'

            row_iter = self.cluster.query(
                sql_query, QueryOptions(positional_parameters=[sample_id]))

            for row in row_iter:
                #print(row["$t"])
                result = (row["$t"]) #only one result line, so it won't be overwritten

            return result
        except Exception as e:
            print(e)

def connection_test():
    #pass
    #couchbaseConnection = CouchbaseConnection('couchbase://localhost:8091', 'admin', 'testpw')
    couchbaseConnection=  CouchbaseConnection('couchbase://138.201.66.27:8091', 'admin', 'testpw')


    couchbaseConnection.lookup_doc_by_id("GSE164728")

    #couchbaseConnection.lookup_characteristics("GSE152189")
    #couchbaseConnection.lookup_characteristics_alternative("GSE150046")

    #couchbaseConnection.lookup_sample_tags("GSM4568913")
    # couchbaseConnection.lookup_sample_tags_alternative("GSM4568913")


    # couchbaseConnection.lookup_sample_id_by_tag("strain")
    #couchbaseConnection.lookup_sample_id_by_tag_alternative("strain")
    #couchbaseConnection.lookup_sample_id_by_tag_optimal("strain")

    # couchbaseConnection.lookup_sample_by_characteristic_alternative("tissue", "zebrafish neuromast hair cells")
    # couchbaseConnection.lookup_series_by_platform_id("GPL25922")

connection_test()
