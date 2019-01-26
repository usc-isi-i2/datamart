import requests
import os
import json
from argparse import ArgumentParser

DEFAULT_KEY = {
    "KEY": 'guest:guest'
}


def getAllIndicatorList():
    indicatorUrlPath = "https://api.tradingeconomics.com/indicators"
    url1 = indicatorUrlPath + '?c=' + DEFAULT_KEY["KEY"] +'&f=json'
    res = requests.get(url1)
    data = res.json()
    url1_list = list(((object['Category'].lower(), object['Category']) for object in data))
    url1_list = set(url1_list)

    indicatorDescriptionUrlPath = "https://api.tradingeconomics.com/indicators/descriptions"

    url2 = indicatorDescriptionUrlPath + '?c=' + DEFAULT_KEY["KEY"]
    res = requests.get(url2)
    data = res.json()
    url2_list = list(((object['URL'].split('/'))[-1].replace('-', ' '), object['Category']) for object in data)
    url2_list = set(url2_list)
    unique_urls_str = list(url2_list.union(url1_list))
    return unique_urls_str


def generate_json_schema(dst_path):
    unique_urls_str = getAllIndicatorList()
    notworking=[]
    for path, indicator in unique_urls_str:
        path = path.replace(' ', '%20')
        materialiseFormat = 'csv'
        infoFormat = 'json'
        url = "https://api.tradingeconomics.com/historical/country/all/indicator/" + path + "?c=" + DEFAULT_KEY[
            "KEY"] + "&format=" + infoFormat
        print("Generating schema for Trading economics", indicator)
        res_indicator = requests.get(url)
        data = res_indicator.json()
        if len(data) > 2:
            schema = dict()
            schema['title'] = data[0]['Category']
            schema['description'] = data[0]['Category'] + " of all country."
            schema['url'] = "https://api.tradingeconomics.com/historical/country/all/indicator/" + path + "?c=" + \
                            DEFAULT_KEY["KEY"] + "&format=" + materialiseFormat
            schema['date_updated'] = data[-1]['LastUpdate']
            schema['provenance'] =  {'source':'tradingeconomics.com'}
            schema['materialization'] = {
                "python_path": 'tradingeconomics_materializer',
                "arguments": {}
            }
            schema['variables'] = []
            first_col = {
                'name': 'Country',
                'description': 'Name of the country',
                'semantic_type': ["http://schema.org/Text"],
                'named_entity': None
            }
            second_col = {
                'name': 'Category',
                'description': 'Indicator name',
                'semantic_type': ["http://schema.org/Text"]
            }
            third_col = {
                'name': 'DateTime',
                'description': 'DateTime of the current value',
                'semantic_type': ["https://metadata.datadrivendiscovery.org/types/Time"],
                "temporal_coverage": {
                    "start": data[0]['DateTime'],
                    "end": data[-1]['DateTime'],
                }
            }
            fourth_col = {
                'name': 'Value',
                'description': data[0]['Category'] + ' value of given DateTime',
                'semantic_type': ["http://schema.org/Float"]
            }
            fifth_col = {
                'name': 'Frequency',
                'description': 'Data frequency',
                'semantic_type': ["http://schema.org/Text"]
            }
            sixth_col = {
                'name': 'LastUpdate',
                'description': 'date when data was last updated',
                'semantic_type': ["https://metadata.datadrivendiscovery.org/types/Time"],
            }
            schema['variables'].append(first_col)
            schema['variables'].append(second_col)
            schema['variables'].append(third_col)
            schema['variables'].append(fourth_col)
            schema['variables'].append(fifth_col)
            schema['variables'].append(sixth_col)
            if dst_path:
                os.makedirs(dst_path + '/tradingecomonics_schema', exist_ok=True)

                file = os.path.join(dst_path, 'tradingecomonics_schema',
                                    "{}_description.json".format(data[0]['Category'].replace(' ', '_')))
            else:
                os.makedirs('tradingecomonics_schema', exist_ok=True)
                file = os.path.join('tradingecomonics_schema',
                                    "{}_description.json".format(data[0]['Category'].replace(' ', '_')))

            with open(file, "w") as fp:
                json.dump(schema, fp, indent=2)
        else:
            notworking.append(path)
    #print(len(notworking),len(unique_urls_str))
    #print(notworking)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-k", "--key", action="store", help='Provide your api key for trading economics', type=str,
                        dest="api_key")
    parser.add_argument("-o", "--dst", action="store", type=str, dest="dst_path")
    args, _ = parser.parse_known_args()
    if args.api_key:
        DEFAULT_KEY["KEY"] = args.api_key
    generate_json_schema(args.dst_path)
