import requests
import os
import json
from argparse import ArgumentParser



def generate_json_schema(dst_path):
    acledUrl="https://api.acleddata.com/acled/read.csv?terms=accept&event_date=2018-01-20&page=1"
    url="https://api.acleddata.com/acled/read?terms=accept&event_date=2018-01-20&page=1"
    res=requests.get(url)
    data = res.json()
    if data['count']>2:
        schema = dict()
        print(data)
        schema = dict()
        schema['title'] = 'ACLED'
        schema['description'] = 'Armed Conflict Location & Event Data Project (ACLED)'
        schema['url'] = acledUrl
        schema['provenance'] =  {'source':'acleddata.com'}
        schema['materialization'] = {
            "python_path": 'acled_materializer',
            "arguments": {}
        }
        schema['variables'] = [
            {
                'name': 'data_id',
                'description': 'A unique id for the row of data',
                'semantic_type': ["http://schema.org/Integer"],
                'named_entity': None
            },
            {
                'name': 'iso',
                'description': 'A numeric code for each individual country. https://www.acleddata.com/download/3987/',
                'semantic_type': ["http://schema.org/Integer"]
            },
            {
                'name': 'event_id_cnty',
                'description': 'An individual identifier by number and country acronym',
                'semantic_type': ["http://schema.org/Text"]
            },
            {
                'name': 'event_id_no_cnty',
                'description':'An individual numeric identifier',
                'semantic_type': ["http://schema.org/Text"]
            },
            {
                'name': 'event_date',
                'description': 'The date the event occurred in the format: yyyy-mm-dd',
                'semantic_type': ["https://metadata.datadrivendiscovery.org/types/Time"]
            },
            {
                'name': 'year',
                'description': 'The year the event occurred.',
                'semantic_type': ["http://schema.org/Integer"]
            },
            {
                'name': 'time_precision',
                'description': 'A numeric code indicating the level of certainty of the date coded for the event',
                'semantic_type': ["http://schema.org/Integer"]
            },
            {
                'name': 'event_type',
                'description': 'The type of conflict event',
                'semantic_type': ["http://schema.org/Text"]
            },
            {
                'name': 'actor1',
                'description': 'The named actor involved in the event',
                'semantic_type': ["http://schema.org/Text"]
            },
            {
                'name': 'assoc_actor_1',
                'description': 'The named actor allied with or identifying ACTOR1',
                'semantic_type': ["http://schema.org/Text"]
            },
            {
                'name': 'inter1',
                'description': 'A numeric code indicating the type of ACTOR1.',
                'semantic_type': ["http://schema.org/Integer"]
            },
            {
                'name': 'actor2',
                'description': 'The named actor involved in the event',
                'semantic_type': ["http://schema.org/Text"]
            },
            {
                'name': 'assoc_actor_2',
                'description': 'The named actor allied with or identifying ACTOR2',
                'semantic_type': ["http://schema.org/Text"]
            },
            {
                'name': 'inter2',
                'description': 'A numeric code indicating the type of ACTOR2.',
                'semantic_type': ["http://schema.org/Integer"]
            },
            {
                'name': 'interaction',
                'description': 'A numeric code indicating the interaction between types of ACTOR1 and ACTOR2',
                'semantic_type': ["http://schema.org/Integer"]
            },
            {
                'name': 'region',
                'description': 'The region in which the event took place',
                'semantic_type': ["http://schema.org/Text"]
            },
            {
                'name': 'country',
                'description': 'The name of the country the event occurred in',
                'semantic_type': ["http://schema.org/Text"]
            },
            {
                'name': 'admin1',
                'description': 'The largest sub-national administrative region in which the event took place',
                'semantic_type': ["http://schema.org/Text"]
            },
            {
                'name': 'admin2',
                'description': 'The second largest sub-national administrative region in which the event took place',
                'semantic_type': ["http://schema.org/Text"]
            },
            {
                'name': 'admin3',
                'description': 'The third largest sub-national administrative region in which the event took place',
                'semantic_type': ["http://schema.org/Text"]
            },
            {
                'name': 'location',
                'description': 'The location in which the event took place',
                'semantic_type': ["http://schema.org/Location"]
            },
            {
                'name': 'latitude',
                'description': 'The latitude of the location',
                'semantic_type': ["http://schema.org/Float"]
            },
            {
                'name': 'longitude',
                'description': 'The longitude of the location',
                'semantic_type': ["http://schema.org/Float"]
            },
            {
                'name': 'geo_precision',
                'description': 'A numeric code indicating the level of certainty of the location coded for the event',
                'semantic_type': ["http://schema.org/Integer"]
            },
            {
                'name': 'source',
                'description': 'The source of the event report',
                'semantic_type': ["http://schema.org/Text"]
            },
            {
                'name': 'source_scale',
                'description': 'The scale of the source',
                'semantic_type': ["http://schema.org/Text"]
            },
            {
                'name': 'notes',
                'description': 'A short description of the event',
                'semantic_type': ["http://schema.org/Text"]
            },
            {
                'name': 'fatalities',
                'description': 'The number of reported fatalities which occurred during the event',
                'semantic_type': ["http://schema.org/Integer"]
            },
            {
                'name': 'timestamp',
                'description': 'The unix timestamp this data entry was last updated',
                'semantic_type': ["http://schema.org/Timeseries"]
            },
            {
                'name': 'iso3',
                'description': 'A 3 character code representation of each country',
                'semantic_type': ["http://schema.org/Text"]
            }
        ]
        if dst_path:
            os.makedirs(dst_path + '/acled_schema', exist_ok=True)

            file = os.path.join(dst_path, 'acled_schema_description.json')
        else:
            os.makedirs('acled_schema', exist_ok=True)
            file = os.path.join('acled_schema',
                                'acled_schema_description.json')

        with open(file, "w") as fp:
            json.dump(schema, fp, indent=2)


if __name__ == '__main__':
    parser = ArgumentParser()

    parser.add_argument("-o", "--dst", action="store", type=str, dest="dst_path")
    args, _ = parser.parse_known_args()

    generate_json_schema(args.dst_path)
