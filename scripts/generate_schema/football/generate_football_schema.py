import http.client
import json, os, sys


def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def obj_getter(uri):
    connection = http.client.HTTPConnection('api.football-data.org')
    headers = {'X-Auth-Token': 'd019bc4541c9490fabcba6806cbcc42b'}
    connection.request('GET', uri, None, headers)
    response = json.loads(connection.getresponse().read().decode())
    return response


def generate_json_schema(uri):
    schema = dict()
    schema['title'] = '2018 World Cup'
    schema['url'] = 'https://www.football-data.org'
    schema['keywords'] = ['football', 'World Cup', 'Russia', '2018']
    schema['provenance'] = {'source': 'www.football-data.org'}
    schema['materialization'] = {
        "python_path": 'football_match_materializer',
        "arguments": {
            "uri": '/v2/competitions/2000/matches?limit=999',
            "token": 'd019bc4541c9490fabcba6806cbcc42b'
        }
    }
    schema['variables'] = []
    all_data = obj_getter(uri)['matches']  # list of matches
    flatten_match = flatten_json(all_data[0])
    variables = {key: {} for key in flatten_match}
    time_col = ['lastUpdated', 'utcDate']
    entity_col = ['referees_' + str(i) + '_name' for i in range(9)]
    entity_col.append('homeTeam_name')
    entity_col.append('awayTeam_name')
    for i in range(len(all_data)):
        flatten_match = flatten_json(all_data[i])
        if i == 0:
            for key in flatten_match:
                variables[key]['name'] = key
                if key in time_col:
                    variables[key]['temporal_coverage'] = None
                if key in entity_col:
                    variables[key]['name_entity'] = set()
        for k, v in flatten_match.items():
            if k in entity_col:
                variables[k]['name_entity'].add(v)
    for key in variables:
        try:
            variables[key]['name_entity'] = list(variables[key]['name_entity'])
        except:
            pass
        schema['variables'].append(variables[key])

    description_path = sys.argv[1]
    os.makedirs(description_path, exist_ok=True)

    with open(os.path.join(description_path, "{}.json".format('2018_world_cup_match_schema')), "w") as fp:
        json.dump(schema, fp, indent=2)


if __name__ == '__main__':
    uri = '/v2/competitions/2000/matches?limit=999'
    generate_json_schema(uri)
