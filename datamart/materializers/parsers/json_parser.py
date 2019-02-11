from pandas.io.json import json_normalize
import json

from datamart.materializers.parsers.parser_base import *


class JSONParser(ParserBase):

    def get_all(self, url: str) -> typing.List[pd.DataFrame]:
        """
        Parses json and returns result

        Params:
            - url: (str)

        Returns:
            - result: (list) 
        """

        data = json.loads(self.load_content(url))

        if type(data) is list:
            data = [self._flatten_dict(x) for x in data]
            return [json_normalize(data)]
        else:
            return [json_normalize(self._flatten_dict(data))]

       
    @staticmethod
    def _flatten_dict(d: dict) -> typing.Dict:
        """
        Flattens a dictionary and returns the flattented dict

        Params:
            - d: (dict) Dictionary to be flattened

        Returns:
            - out: (dict) Flattened dictionary
        """
        out = {}
        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '.')
            elif type(x) is list:
                i = 0
                for a in x:
                    flatten(a, name + str(i) + '.')
                    i += 1
            else:
                out[name[:-1]] = x  

        flatten(d)
        return out