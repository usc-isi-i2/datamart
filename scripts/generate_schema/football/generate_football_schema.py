import http.client
import json, os, sys
import time


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
    time.sleep(6)
    return response


def get_comp_id(uri):
    res = obj_getter(uri)
    competitions = res['competitions']
    id_list = []
    for competition in competitions:
        id_list.append(competition['id'])
    return id_list


def generate_json_schema(uri):
    res = obj_getter(uri)
    if len(res) == 2:
        print('Restricted uri:', uri)
        return False
    print('Valid uri:', uri)
    all_data = res['matches']
    schema = dict()
    schema['title'] = res['competition']['name']
    schema['description'] = res['competition']['name']
    schema['url'] = 'https://www.football-data.org'
    schema['keywords'] = ['football', 'competition']
    schema['provenance'] = {'source': 'www.football-data.org'}
    schema['materialization'] = {
        "python_path": 'football_match_materializer',
        "arguments": {
            "uri": uri,
            "token": 'd019bc4541c9490fabcba6806cbcc42b'
        }
    }
    schema['variables'] = []
    flatten_match = flatten_json(all_data[0])
    variables = {key: {} for key in flatten_match}
    time_col = ['lastUpdated', 'utcDate']
    entity_col = ['referees_' + str(i) + '_name' for i in range(len(all_data[0]['referees']))]
    entity_col.append('homeTeam_name')
    entity_col.append('awayTeam_name')
    str_type_col = ['referees_' + str(i) + '_name' for i in range(len(all_data[0]['referees']))]
    str_type_col.extend(
        ['status', 'stage', 'group', 'score_winner', 'score_duration', 'homeTeam_name', 'awayTeam_name'])
    int_type_col = [x for x in flatten_match.keys() if 'id' in x]
    int_type_col.extend(
        ['score_fullTime_homeTeam', 'score_fullTime_awayTeam', 'score_halfTime_homeTeam', 'score_halfTime_awayTeam'])
    date_type_col = [x for x in flatten_match.keys() if 'date' in x or 'Date' in x]
    for i in range(len(all_data)):
        flatten_match = flatten_json(all_data[i])
        if i == 0:
            for key in flatten_match:
                variables[key]['name'] = key
                if key in time_col:
                    variables[key]['temporal_coverage'] = None
                if key in entity_col:
                    variables[key]['named_entity'] = set()
                if key in str_type_col:
                    variables[key]['semantic_type'] = ["http://schema.org/Text"]
                if key in int_type_col:
                    variables[key]['semantic_type'] = ["http://schema.org/Integer"]
                if key in date_type_col:
                    variables[key]['semantic_type'] = ["https://metadata.datadrivendiscovery.org/types/Time"]

        for k, v in flatten_match.items():
            if k in entity_col:
                variables[k]['named_entity'].add(v)
    for key in variables:
        try:
            variables[key]['named_entity'] = list(variables[key]['named_entity'])
        except:
            pass
        schema['variables'].append(variables[key])

    comp_name = schema['title']
    name = comp_name.replace(' ', '_')

    description_path = sys.argv[1]
    os.makedirs(description_path, exist_ok=True)

    with open(os.path.join(description_path, "{}_match_schema.json".format(name)), "w") as fp:
        json.dump(schema, fp, indent=2)

    # with open("{}_match_schema.json".format(name), "w") as fp:
    #     json.dump(schema, fp, indent=2)


if __name__ == '__main__':
    uri = '/v2/competitions/'
    comp_id = get_comp_id(uri)
    print('# of uri:', len(comp_id))
    for id in comp_id:
        uri = '/v2/competitions/' + str(id) + '/matches?limit=999'
        generate_json_schema(uri)
