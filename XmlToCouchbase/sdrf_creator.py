import re

from couchbase_handler import CouchbaseConnection



def create_sdrf():
    # change dir
    cb = CouchbaseConnection('couchbase://localhost:8091', 'admin', 'testpw')

    title_list = ["embryo_age", "dechorinated", "exposure_start", "exposure_duration", "concentration",  "compound", "tissue"]
    # tags have to be in the same order as corresponding titles
    tag_list = ["age_raw", "genotype_raw", "treatment_raw"]

    f = open("C:/Users/Gsell/PycharmProjects/XmlToCouchbase/test_file.txt", "w")
    # write headers

    for title in title_list: f.write("\""+title+"\"" + " ")

    # write id's of all samples to sdrf file
    sample_id_list = cb.lookup_sample_ids("GSE37019")
    for id in sample_id_list:
        f.write("\n")
        f.write("\"" + str(id) + "\"")

        # ohne "vereinheitlichung" der tags müsste hier dann auf die synonym liste zugegriffen werden

        # write data for every tag specified in tag_list to sdrf file
        for tag in tag_list:
            tag_content = cb.lookup_tag_content(id, tag)

            # create harmonized content for age_raw (i.e. X hpf)
            if tag == "age_raw":
                tag_content = parse_for_hpf(tag_content)
                f.write(" " + "\"" + str(tag_content) + "\"")
            if tag == "treatment_raw":
                treatment_raw = parse_for_concentration(tag_content)
                # todo hier überlegen wie ich es mache, wegen der reihenfolge -> es werden aus einem content zwei einträge
                concentration = treatment_raw[0]
                f.write(" " + "\"" + str(concentration) + "\"")
                compound = treatment_raw[1]
                f.write(" " + "\"" + str(compound) + "\"")

            else:
                f.write(" " + "\"" + str(tag_content) + "\"")

    f.close()
    #write age_raw




def parse_for_hpf(text):

    numbers_and_units = re.findall(r'(\d+\.*\d*\s?)([a-zA-Z]*)', text)
    # #insert spaces if not there
    # for i in numbers_and_units:
    #     result = re.sub(r'(\d)([a-zA-Z])', r'\1 \2', i)

    print(numbers_and_units)

def parse_for_concentration(text):
    #todo: wie den verwendeten stoff isolieren? oft steht noch unnötige information dabei. Kann nicht davon ausgehen dass wort nach der Mengen angabe immer der stoff ist, da stoff auch mehrere worte haben kann oder vor der angabe stehen kann
    #todo bräuchte idealerweise eine liste aller bisher vorkommenden stoffe, die ich füttern kann um diesen zu isolieren. -> geht nicht anders.

    #TEST
    # remove time entries from text, since sometimes times pollute the data
    text_without_time = re.sub(r'(\d+\.*\d*\s?(millisecond|second|hour|minute|milliseconds|seconds|hours|minutes|sec|min|ms\b|s\b|m\b|h\b))', r'', text)

    # ANNAHME: Zahlen IMMER mit englischer Punktuation, units always follow numbers, with one or less spaces inbetween. units contain no spaces.
    # 1-n Zahlen, 0-n Punkte , 0 bis n zahlen, 0 bis 1 whitespace, 0-n buchstaben, 0-1 /, 0-n buchstaben
    # ggf. noch um sondercharaktere erweitern (müh)
    # finde alle mengen + unit angaben (Problem: er würde auch Angaben über längen finden z.B. 8h -> definieren welche buchstaben nicht als mengen operator vorkommen können (s, sec, m, min, h, hours)
    numbers_and_units = re.findall(r'(\d+\.*\d*\s?)([a-zA-Z]*[/]?[a-zA-Z]*)', text_without_time)

    #remove numbers_and_units from text, rest is compound
    compound = re.sub(r'\d+\.*\d*\s?[a-zA-Z]*[/]?[a-zA-Z]*',r'',text_without_time)

    # #insert spaces if not there
    # for i in numbers_and_units:
    #     result = re.subres = re.sub(r'(\d)([a-zA-Z])', r'\1 \2', i)

    # assumption: only one unit couple
    if numbers_and_units:
        concentration_number = numbers_and_units[0][0].split()[0]
        concentration_unit = numbers_and_units[0][1].split()[0]
        numbers_and_units = concentration_number + " " + concentration_unit
    else:
        print("no concentrations and units found")

    #concentration_number, concentration_unit = result.split()

    result = (numbers_and_units, compound)

    print(type(result[1]))
    #print(concentration_number)
    #print(concentration_unit)

def parse_for_exposure_duration(text):
    #extract time dates. eg. 30 min, 0.5h, 30s, ...
    #FIXME fix so that decimals are also found!
    time_and_unit = re.findall(r'(\d+\.*\d*\s?)(millisecond|second|hour|minute|milliseconds|seconds|hours|minutes|sec|min|ms\b|s\b|m\b|h\b)', text)

    print(time_and_unit)


def calculate_exposure_start(age, age_unit, duration, duration_unit):
    # abbreviations that are checked to determine the unit
    second_names = ["s", "s.", "sec", "sec.", "secs", "second", "seconds"]
    minute_names = ["m", "m.", "min", "min.", "mins", "minute", "minutes"]
    hour_names = ["h", "h.", "hour", "hours"]

    # transform strings to floats
    age, duration = float(age), float(duration)

    # todo test
    if age_unit in second_names:
        age_in_hours = age/3600
    elif age_unit in minute_names:
        age_in_hours = age/60
    elif age_unit in hour_names:
        age_in_hours = age

    if duration_unit in second_names:
        duration_in_hours = duration/3600
    elif duration_unit in minute_names:
        duration_in_hours = duration/60
    elif duration_unit in hour_names:
        duration_in_hours = duration

    # im endeffekt embryo age - factor duration (jetzt mal als Annahme)
    exposure_start_in_hours = age_in_hours - duration_in_hours
    exposure_start_in_hours = round(exposure_start_in_hours, 4)
    print(str(exposure_start_in_hours) + " " + "hours")



# test_cases = ["0.2mg ist gut 2m", "0.2mg ist gut 3hours", "0.2 mg/l ist gut 243 m", "3mg/l "]
#
# for i in test_cases: parse_for_concentration(i)

parse_for_hpf("embryos at about 8hpf (gastrula)")
# #parse_for_exposure_duration("30min. egg collections at 25C were exposed in pentachlorophenol solution according to "
#                             "the different concentrations C1-C3. Embryos of the appropriate stage were manually selected "
#                             "under the dissecting scope. Selected embryos were rinsed with PBS and then transferred to a microtube"
#                             " filled with Trizol solution (Invitrogen).")


#calculate_exposure_start("50", "hours", "2000", "min")



#Erfragen: Wo kommt Wert für dechorinated her? Gibt es Liste mit allen compounds die verwendet werden? Ist Deutsche Kommasetzung für dezimalzahlen erforderlich?
    #write dechorinated. Email morgen raushauen





#todo create function to extract hpf in correct format
#todo create function to seperate concentration from compound
#schreiben ob es gesamte liste gibt von daten die potentiell extrahiert werden müssen => wo finde ich z.B. dechorinated?
#wird exposure start aus exposure duration - 30 min aus treatment protocol ausgelesen?
# ideal wäre liste mit inhaltlichen kombinationen die vorkommen können

#create_sdrf()

#"C:/Users/Gsell/PycharmProjects/XmlToCouchbase"