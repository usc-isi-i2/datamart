import os
from argparse import ArgumentParser
import requests
import json

DEFAULT_KEY = {

}


def getAllIndicatorList():
    url = "https://api.worldbank.org/v2/indicators?format=json&page=1&per_page=2"
    res = requests.get(url)
    data = res.json()
    return data[1]


def generate_json_schema(dst_path):
    unique_urls_str = getAllIndicatorList()
    # print(unique_urls_str)
    for commondata in unique_urls_str:
        # print('here',commondata)
        # commondata=data[0]
        # print(commondata['id'])
        urldata = "https://api.worldbank.org/v2/countries/br/indicators/" + commondata['id'] + "?format=json"
        resdata = requests.get(urldata)
        data_ind = resdata.json()
        # print(data_ind)
        materialiseFormat = 'csv'
        infoFormat = 'json'
        print("Generating schema for Trading economics", commondata['name'])
        schema = {}
        schema["title"] = commondata['name']
        schema["description"] = commondata['sourceNote']
        schema["url"] = "https://api.worldbank.org/v2/indicators/" + commondata['id'] + "?format=json"
        schema["keywords"] = [i for i in commondata['name'].split()]
        schema["date_updated"] = data_ind[0]["lastupdated"]
        schema["license"] = None
        schema["provenance"] = "http://worldbank.org"
        schema["original_identifier"] = commondata['id']

        schema["materialization"] = {
            "python_path": "worldbank_materializer",
            "arguments": {
                "url": "https://api.worldbank.org/v2/indicators/" + commondata['id'] + "?format=json"
            }
        }
        schema['variables'] = []
        first_col = {
            "name": "indicator.id",

            "description": "id is identifier of an indicator in worldbank datasets",
            "semantic_type": ["https://metadata.datadrivendiscovery.org/types/CategoricalData"]
        }
        second_col = {
            "name": "indicator.value",

            "description": "name of an indicator in worldbank datasets",
            "semantic_type": ["http://schema.org/Text"]
        }
        third_col = {
            "name": "unit",

            "description": "unit of value returned by this indicator for a particular country",
            "semantic_type": ["https://metadata.datadrivendiscovery.org/types/CategoricalData"]

        }
        fourth_col = {
            "name": "sourceNote",

            "description": "Long description of the indicator",
            "semantic_type": ["http://schema.org/Text"]
        }
        fifth_col = {
            "name": "sourceOrganization",
            "description": "Source organization from where Worldbank acquired this data",
            "semantic_type": ["http://schema.org/Text"]
        }
        sixth_col = {
            "name": "country.value",

            "description": "Country for which idicator value is returned",
            "semantic_type": ["https://metadata.datadrivendiscovery.org/types/Location"],
            "named_entity": None
        }
        seventh_col = {
            "name": "countryiso3code",

            "description": "Country iso code for which idicator value is returned",
            "semantic_type": ["https://metadata.datadrivendiscovery.org/types/Location"]
        }
        eighth_col = {
            "name": "date",

            "description": "date for which indictor value is returned for a particular country",
            "semantic_type": ["https://metadata.datadrivendiscovery.org/types/Time"],
            "temporal_coverage": {"start": None, "end": None}

        }
        schema['variables'].append(first_col)
        schema['variables'].append(second_col)
        schema['variables'].append(third_col)
        schema['variables'].append(fourth_col)
        schema['variables'].append(fifth_col)
        schema['variables'].append(sixth_col)
        schema['variables'].append(seventh_col)
        schema['variables'].append(eighth_col)
        if dst_path:
            os.makedirs(dst_path + '/WorldBank_schema', exist_ok=True)

            file = os.path.join(dst_path, 'WorldBank_schema',
                                "{}_description.json".format(commondata['id']))
        else:
            os.makedirs('WorldBank_schema', exist_ok=True)
            file = os.path.join('WorldBank_schema',
                                "{}_description.json".format(commondata['id']))

        with open(file, "w") as fp:
            json.dump(schema, fp, indent=2)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-o", "--dst", action="store", type=str, dest="dst_path")
    args, _ = parser.parse_known_args()
    generate_json_schema(args.dst_path)