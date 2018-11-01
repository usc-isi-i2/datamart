import psycopg2
import requests
import json
import os
import sys
import json

DBNAME = 'faostat'
USER = 'postgres'
PASSWORD = '123123'
HOST = 'dsbox02.isi.edu'
JSONDESCRIPTION = "http://fenixservices.fao.org/faostat/static/bulkdownloads/datasets_E.json"
SPECIAL_URL = "http://fenixservices.fao.org/faostat/static/bulkdownloads/Environment_LivestockManure_E_All_Data_(Normalized).zip"


def generate_json_schema():
    conn = None
    response = json.loads(requests.get(JSONDESCRIPTION).text)
    data_sets = response['Datasets']['Dataset']
    description = dict()
    keyword = dict()
    for data in data_sets:
        url = data["FileLocation"]
        tmp = url.split("/")
        words = tmp[len(tmp) - 1].split(".")[0].split("_")
        name = ""
        if url == SPECIAL_URL:
            name = "environment_manure_e_all_data"
        else:
            for word in words:
                if word.find("(") == -1:
                    name = name + word + "_"
            name = name[:-1].lower().replace("-", "_")

        description[name] = data["DatasetDescription"]
        if data["Topic"] is None:
            keyword[name] = None
        else:
            keyword[name] = [data["Topic"]]
    try:
        # read connection parameters
        params = dict()
        params["dbname"] = DBNAME
        params["user"] = USER
        params["password"] = PASSWORD
        params["host"] = HOST

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        # create a cursor
        cur = conn.cursor()
        cur.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public'""")

        for table in cur.fetchall():
            name = str(table)[2: -3]
            cur.execute("Select * FROM {0} limit 1".format(name))
            colnames = [desc[0] for desc in cur.description]
            for col in colnames:
                if col == 'elementcode':
                    cur.execute('DROP TABLE "{0}";'.format(name))
                    conn.commit()

            schema = dict()
            schema['title'] = name
            schema['description'] = description[name]
            schema['url'] = 'http://fenixservices.fao.org/faostat/static/bulkdownloads/datasets_E.json'
            schema['keywords'] = keyword[name]
            schema['provenance'] = {'source': 'fao.org'}
            schema['materialization'] = {
                "python_path": 'fao_materializer',
                "arguments": {
                    "type": name
                }
            }
            resources_path = os.path.join(os.path.dirname(__file__), "UN's_region_for_FAO.json")
            schema['variables'] = []
            first_col = dict()
            first_col['name'] = colnames[0]
            first_col['description'] = 'the area of data'
            first_col['semantic_type'] = ["https://metadata.datadrivendiscovery.org/types/Location"]
            with open(resources_path) as f:
                first_col['named_entity'] = json.load(f)["countries"]

            sec_col = dict()
            sec_col['name'] = colnames[1]
            sec_col['description'] = colnames[1]
            sec_col['semantic_type'] = ["http://schema.org/Text"]

            third_col = dict()
            third_col['name'] = colnames[2]
            third_col['description'] = colnames[2]
            third_col['semantic_type'] = ["http://schema.org/Text"]

            fourth_col = dict()
            fourth_col['name'] = colnames[3]
            fourth_col['description'] = 'the year of data'
            fourth_col['semantic_type'] = ["http://schema.org/Integer"]
            fourth_col['temporal_coverage'] = None

            fif_col = dict()
            fif_col['name'] = colnames[4]
            fif_col['description'] = 'the value of this type of data'
            fif_col['semantic_type'] = ["http://schema.org/Float"]

            schema['variables'].append(first_col)
            schema['variables'].append(sec_col)
            schema['variables'].append(third_col)
            schema['variables'].append(fourth_col)
            schema['variables'].append(fif_col)

            description_path = sys.argv[1]
            os.makedirs(description_path, exist_ok=True)
            with open(os.path.join(description_path, "{}_description.json".format(name)), "w") as fp:
                json.dump(schema, fp, indent=2)
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    generate_json_schema()
