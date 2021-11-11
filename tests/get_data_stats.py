# aus der JSON Datei einfach Length von sample array auslesen
# erstmal alle json dateien einlesen // oder über xml machen
import os

import lxml.etree as ET

directory_in_str = "C:/Users/Gsell/Documents/danio_rerio_benchmark"


with os.scandir(directory_in_str) as it:
    for entry in it:
        if entry.name.endswith(".xml") and entry.is_file():

            contributor_count = 0
            sample_count = 0
            series_count = 0
            platform_count = 0
            #print("-------------------------------------------------")
            # parse xml file

            # print(entry.name + " " + str(my_schema.is_valid(entry.path)))

            tree = ET.parse(entry.path)
            root = tree.getroot()
            # convert xml to json

            for elem in root.getiterator():
                tag = ET.QName(elem).localname

                if tag == "Sample":
                    sample_count += 1

                if tag == "Platform":
                    platform_count += 1

                if tag == "Series":
                    series_count += 1

                if tag == "Contributor":
                    contributor_count += 1


            print(series_count)
                #print(tag)
                # count contributors
                # count samples



