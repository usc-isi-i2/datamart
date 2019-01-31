import requests
import os
import json
from argparse import ArgumentParser
import csv


DEFAULT_KEY = {
    "KEY": 'guest:guest'
}


def getAllIndicatorList(UrlPath):
    url = UrlPath + '?c=' + DEFAULT_KEY["KEY"]
    res = requests.get(url)
    data = res.json()
    url_list = list(((object['Symbol'], object['Name'],object["Country"]) for object in data))
    unique_urls_str = list(set(url_list))
    return unique_urls_str

def getStockPathList():
    stockCSV="All_Stocks_Symbols_with_Historical_Data-Updated 2018-11-02.csv"
    paths=[]
    with open(stockCSV) as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        for row in readCSV:
            paths.append((row[0],"",""))
    return paths

def generate_json_schema(dst_path):
    urlType={
        "commoditiesUrlPath" : "https://api.tradingeconomics.com/markets/commodities",
        "currencyUrlPath" : "https://api.tradingeconomics.com/markets/currency",
        "indexUrlPath" : "https://api.tradingeconomics.com/markets/index",
        "bondUrlPath" : "https://api.tradingeconomics.com/markets/bond"
    }

    for type in urlType:
        unique_urls_str = getAllIndicatorList(urlType[type])
        stockPath=getStockPathList()
        for path, name, country in unique_urls_str+stockPath:
            materialiseFormat = 'csv'
            infoFormat = 'json'
            url = "https://api.tradingeconomics.com/markets/historical/" + path.lower() + "?c=" + DEFAULT_KEY[
                "KEY"] + "&format=" + infoFormat +'&d1=1700-08-01'
            try:
                res_indicator = requests.get(url)
                data = res_indicator.json()
            except Exception as e:
                print("Invalid Url for ",path,url)
                continue
            print("Generating schema for Trading economics", path,name,country)
            if len(data) > 2:
                schema = dict()
                schema['title'] = data[0]['Symbol']
                schema['description'] = path + " : " + name +' '+ country
                schema['url'] = "https://api.tradingeconomics.com/markets/historical/" + path.lower() + "?c=" + DEFAULT_KEY["KEY"] +'&d1=1700-08-01'+ "&format=" + materialiseFormat
                schema['date_updated'] = data[0]['Date']
                schema['provenance'] = {'source':'tradingeconomics.com'}
                schema['materialization'] = {
                    "python_path": 'tradingeconomics_market_materializer',
                    "arguments": {}
                }
                schema['variables'] = []
                first_col = {
                    'name': 'Symbol',
                    'description': 'Symbol of the market',
                    'semantic_type': ["http://schema.org/Text"]
                }
                second_col = {
                    'name': 'Date',
                    'description': 'Date of the current value',
                    'semantic_type': ["https://metadata.datadrivendiscovery.org/types/Time"],
                    "temporal_coverage": {
                        "start": None,
                        "end": None,
                    }
                }
                third_col = {
                    'name': 'Open',
                    'description': 'Market Open value on given Date',
                    'semantic_type': ["http://schema.org/Float"]
                }
                fourth_col = {
                    'name': 'High',
                    'description': 'Market High value on given Date',
                    'semantic_type': ["http://schema.org/Float"]
                }
                fifth_col = {
                    'name': 'Low',
                    'description': 'Market Low value on given Date',
                    'semantic_type': ["http://schema.org/Float"]
                }
                sixth_col = {
                    'name': 'Close',
                    'description': 'Market Close value on given Date',
                    'semantic_type': ["http://schema.org/Float"]
                }
                schema['variables'].append(first_col)
                schema['variables'].append(second_col)
                schema['variables'].append(third_col)
                schema['variables'].append(fourth_col)
                schema['variables'].append(fifth_col)
                schema['variables'].append(sixth_col)
                if dst_path:
                    os.makedirs(dst_path + '/tradingecomonics_market_schema', exist_ok=True)

                    file = os.path.join(dst_path, 'tradingecomonics_market_schema',
                                        "{}_description.json".format(path.lower().replace(":", "_").replace(" ", "_")))
                else:
                    os.makedirs('tradingecomonics_schema', exist_ok=True)
                    file = os.path.join('tradingecomonics_schema',
                                         "{}_description.json".format(path.lower().replace(":", "_").replace(" ", "_")))

                with open(file, "w") as fp:
                    json.dump(schema, fp, indent=2)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-k", "--key", action="store", help='Provide your api key for trading economics', type=str,
                        dest="api_key")
    parser.add_argument("-o", "--dst", action="store", type=str, dest="dst_path")
    args, _ = parser.parse_known_args()
    if args.api_key:
        DEFAULT_KEY["KEY"] = args.api_key
    generate_json_schema(args.dst_path)
