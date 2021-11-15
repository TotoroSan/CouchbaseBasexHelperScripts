import os
import time

import XmlToCouchbase.BaseXClient as BaseXClient
import XmlToCouchbase.couchbase_handler


class BaseXConnection(object):

    def __init__(self, host, port, username, password):

        self.host = host
        self.port = port
        self.username = username
        self.password = password


        # create session
        self.session = BaseXClient.Session(host, port, username, password)

    # done -> ready for benchmark
    def lookup_doc_by_id(self, id):
        """Retrieve full document by Series ID"""
        try:
            # create query instance
            input2= "declare variable $id external;(db:open(\"danio_rerio_benchmark\"))//MINiML/Series[@iid=$id]/ancestor-or-self::MINiML"
            query = self.session.query(input2)


            query.bind("$id", id)
            # loop through all results
            for item in query.iter(): print(item)

            # close query object
            query.close()

        finally:
            pass
        #     # close session
        #     if self.session:
        #         self.session.close()

    def lookup_doc_by_id_2(self):
        try:
            # create query instance
            # FIXME how to print content? -> compare to lookup_doc_by_id if there is performance difference
            input2= "(db:open(\"danio_rerio_benchmark\"))(for $x in //MINiML where $x/Series[@iid=\"GSE137770\"] return $x)"
            query = self.session.query(input2)


            # loop through all results
            for item in query.iter():
                print("item=%s" % item)

            # close query object
            query.close()

        finally:
            # close session
            if self.session:
                self.session.close()

    def lookup_characteristics(self, id):
        """"lookup characteristics data of all samples for given series ID"""
        #11.6 MS as unoptimized query
        try:
            # create query instance
            #input2= "declare variable $id external;(db:open(\"danio_rerio_benchmark\"))(for $x in //MINiML where $x/Series[@iid=$id] return <xml>{$x}</xml>)"
            input = "declare variable $id external;db:attribute(\"danio_rerio_benchmark\", $id)" \
                    "/self::attribute(iid)/parent::Series/parent::*:MINiML/Sample/Channel/Characteristics"
            query = self.session.query(input)

            query.bind("$id", id)

            # loop through all results
            #for item in query.iter(): print("item=%s" % item)

            # close query object
            query.close()

        finally:
            pass
            # # close session
            # if self.session:
            #     self.session.close()

    def lookup_sample_tags(self, id):
        """lookup the characteristic tags for given sample ID"""
        # gibt derzeit tag + content aus - abändern wenn ich nur tag möchte
        try:
            # create query instance
            input = "declare variable $id external;db:attribute(\"danio_rerio_benchmark\", $id)" \
                    "/self::attribute(iid)/parent::Sample/Channel/Characteristics ! [@tag]"
            query = self.session.query(input)

            query.bind("$id", id)

            # loop through all results
            #for item in query.iter(): print("item=%s" % item)

            # close query object
            query.close()

        finally:
            pass
            # # close session
            # if self.session:
            #     self.session.close()

    def lookup_sample_id_by_tag(self, tag):
        """lookup ids of samples that have specified tag"""

        try:
            # create query instance
            input = "declare variable $tag external;db:attribute(\"danio_rerio_benchmark\", $tag)" \
                    "/self::attribute(tag)/parent::Characteristics/parent::Channel/parent::Sample/parent::*:MINiML/Sample ! [ @iid ]"
            query = self.session.query(input)

            query.bind("$tag", tag)

            # loop through all results
            #for item in query.iter(): print("item=%s" % item)

            # close query object
            query.close()

        finally:
            pass
            # # close session
            # if self.session:
            #     self.session.close()

    def lookup_series_by_platform_id(self, id):
        """lookup all series ids for documents that use the specified platform"""

        try:
            # create query instance
            input = "declare variable $id external;db:attribute(\"danio_rerio_benchmark\", $id)" \
                    "/self::attribute(iid)/parent::Platform/parent::*:MINiML/Series ! [ @iid ]"
            query = self.session.query(input)

            query.bind("$id", id)

            # loop through all results
            #for item in query.iter(): print("item=%s" % item)

            # close query object
            query.close()

        finally:
            pass
            # # close session
            # if self.session:
            #     self.session.close()

    def lookup_sample_by_characteristic(self, tag, content):
        """lookup sample ids for which passed tag has passed content """

        try:
            # create query instance
            input = "declare variable $tag external;declare variable $content external;" \
                    "db:attribute(\"danio_rerio_benchmark\", $tag)" \
                    "/self::attribute(tag)/parent::Characteristics/parent::" \
                    "Channel/parent::Sample/parent::*:MINiML[(Sample/Channel/Characteristics = $content)]/Sample ! [ @iid ]"
            query = self.session.query(input)

            query.bind("$tag", tag)
            query.bind("$content", content)

            # loop through all results
            #for item in query.iter(): print("item=%s" % item)

            # close query object
            query.close()

        finally:
            pass
            # # close session
            # if self.session:
            #     self.session.close()


bc = BaseXConnection('localhost', 1984, 'admin', 'admin')
#bc.lookup_doc_by_id("GSE137770")
#bc.lookup_doc_by_id()
#bc.lookup_characteristic_by_id("GSE137770")
#bc.lookup_sample_tags("GSM4087122")
#bc.lookup_sample_id_by_tag("line")
#bc.lookup_series_by_platform_id("GPL24995")
#bc.lookup_sample_by_characteristic("tissue", "tail")
