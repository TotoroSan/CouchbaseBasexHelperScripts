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
    sample_id_list = cb.lookup_sample_ids()
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
    """search text for a number and concatenates it with hpf"""
    #Ideal: Liste mit allen verschiedenen variationen an Text die vorkommen können -> unrealistisch
    #Daher: Annahmen -> Wenn eine Zahl vorkommt, ist dies die hpf (testen + absprechen)
    #Potentielle Probleme: mehrere Zahlen, keine Zahlen sondern hpf ist als wort geschrieben i.e. "eight"

    hpf_number = []
    for word in text.split():
        for character in word: # need to parse every character since spaces between numbers and words can be missing
            if character.isdigit():
                hpf_number.append(character)

    # for testing purposes, to see where assumption runs into trouble:
    if len(hpf_number) == 0: print("warning - no integers found - dirty data might be produced")
    if len(hpf_number) == 1: return hpf_number[0] + " " + "hpf"
    if len(hpf_number) > 1: print("warning - multiple integers found - dirty data might be produced")


def parse_for_concentration(text):
    """search text for a number and return number + following word, assuming it is the unit"""
    #assumption: amount and unit are separated by spaces -> if number is found take number and the following word
    #todo use regular expressions to prevent problem if amount and unit are not seperated by spaces
    # todo überarbeiten, da folgende probleme: was wenn keine concentration? oben fixen, was wenn mehrere wörter für compound (e.g. ionized water)

    words = text.split()
    result = [] #index 0 = concentration, index 1 = compound
    for index, word in enumerate(words):
        if (index + 1 < len(words)):
            if word.isdigit():
                concentration = word + " " + words[index+1] # return amount + unit
                result.append(concentration)
                del words[index + 1] # remove larger index first to maintain right order
                del words[index]   # remove concentration so that only amount is left



    if words:
        result.append(words[0]) # append compound to result

        return result
    else: #todo remove // only for debugging
        print("warning - no compound left in list - dirty data")




    #write dechorinated
    #write exposure_start
    #write exposure duration
    #write concentration
    #write tissue
    #write compound





#todo create function to extract hpf in correct format
#todo create function to seperate concentration from compound
#schreiben ob es gesamte liste gibt von daten die potentiell extrahiert werden müssen => wo finde ich z.B. dechorinated?
#wird exposure start aus exposure duration - 30 min aus treatment protocol ausgelesen?
# ideal wäre liste mit inhaltlichen kombinationen die vorkommen können

create_sdrf()

#"C:/Users/Gsell/PycharmProjects/XmlToCouchbase"