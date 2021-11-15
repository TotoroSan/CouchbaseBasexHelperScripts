from couchbase_handler import CouchbaseConnection



def create_sdrf():
    # change dir
    cb = CouchbaseConnection('couchbase://localhost:8091', 'admin', 'testpw')

    title_list = ["embryo_age", "dechorinated", "exposure_start", "exposure_duration", "concentration", "tissue", "compound"]
    # tags have to be in the same order as corresponding titles
    tag_list = ["age_raw"]#, "genotype_raw", "treatment_raw"]

    f = open("C:/Users/Gsell/PycharmProjects/XmlToCouchbase/test_file.txt", "w")
    # write headers

    for title in title_list: f.write("\""+title+"\"" + " ")

    # write id's of all samples of the document to sdrf file
    sample_id_list = cb.lookup_sample_ids("GSE37019")
    print(sample_id_list)
    for id in sample_id_list:
        f.write("\n")
        f.write("\"" + str(id[0]) + "\"")

        # write data for every tag specified in tag_list to sdrf file
        #for tag in tag_list:

        # age_raw
        age_raw = cb.lookup_sample_tag_content_harmonized(id[0], "age_raw")
        f.write(" " + "\"" + str(age_raw) + "\"")

        #dechorinated
        f.write(" " + "\"" + "no" + "\"")

        #exposure_start
        exposure_start = cb.lookup_exposure_start_harmonized(id[0])
        f.write(" " + "\"" + str(exposure_start) + "\"")

        #exposure_duration
        exposure_duration = cb.lookup_exposure_duration_harmonized(id[0])
        f.write(" " + "\"" + str(exposure_duration) + "\"")

        #get concentration
        concentration = cb.lookup_sample_concentration_harmonized(id[0])
        f.write(" " + "\"" + str(concentration) + "\"")

        #get tissue
        tissue = cb.lookup_sample_tag_content_harmonized(id[0], "tissue")
        f.write(" " + "\"" + str(tissue) + "\"")

        #get compound
        compound = cb.lookup_sample_compound_harmonized(id[0])
        f.write(" " + "\"" + str(compound) + "\"")


    f.close()
    #write age_raw

create_sdrf()