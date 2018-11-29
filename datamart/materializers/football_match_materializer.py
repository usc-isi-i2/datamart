from datamart.materializers.materializer_base import MaterializerBase
import pandas as pd
import http.client
import json
import time

DEFAULT_TOKEN = "d019bc4541c9490fabcba6806cbcc42b"
DEFAULT_URI = "/v2/competitions/2000/matches?limit=999"


class FootballMatchMaterializer(MaterializerBase):
    """
    FootballMatchMaterializer class extended from  Materializer class
    """

    def __init__(self, **kwargs):
        MaterializerBase.__init__(self, **kwargs)

    def flatten_json(self, y):
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

    def curl_wrapper(self, token, uri):
        token = token
        api_str = 'api.football-data.org'
        connection = http.client.HTTPConnection(api_str)
        headers = {'X-Auth-Token': token}
        connection.request('GET', uri, None, headers)
        response = json.loads(connection.getresponse().read().decode())
        time.sleep(6)  # maximum 10 calls per a minute
        return response

    def csv_generator(self, response):
        matches_dicts = response.get('matches', {})
        matches = list()
        for match in matches_dicts:
            flatten_match = self.flatten_json(match)
            matches.append(flatten_match)
        df = pd.DataFrame(matches)
        return df

    def get(self, metadata: dict() = None, constrains: dict() = None):
        materialization_arguments = metadata["materialization"].get("arguments", {})
        token = materialization_arguments.get("token", DEFAULT_TOKEN)
        uri = materialization_arguments.get("uri", DEFAULT_URI)
        response = self.curl_wrapper(token, uri)
        df = self.csv_generator(response)
        return df.reindex([x["name"] for x in metadata["variables"]], axis=1)
