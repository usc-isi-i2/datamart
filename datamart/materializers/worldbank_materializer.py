from materializer_base import MaterializerBase
import pandas as pd
import datetime
import csv
import requests
import typing
import os
import json
import pandas as pd
from pandas.io.json import json_normalize


DEFAULT_LOCATIONS = [
    "Africa"
]

class WorldBankMaterializer(MaterializerBase):

    def __init__(self):
        MaterializerBase.__init__(self)
        self.headers = None
        resources_path = os.path.join(os.path.dirname(__file__), "../resources")
        with open(os.path.join(resources_path, 'country_to_id.json'), 'r') as json_file:
            reader = csv.reader(json_file)
            self.country_to_id_map = dict(reader)

    def get(self, metadata: dict = None, variables: typing.List[int] = None, constrains: dict = None) -> pd.DataFrame:
        if not constrains:
            constrains = dict()
        date_range = constrains.get("date_range", {"start": "2002","end": "2003"})
        locations = constrains.get("locations", ['Aruba','Africa'])
        dataset_id = constrains.get("dataset_id", 'NY.GDP.MKTP.CD')
        return self.fetch_data(date_range=date_range, locations=locations,dataset_id=dataset_id)

    def fetch_data(self, date_range: dict = None, locations: list = ['Aruba','Africa'], dataset_id: str = 'NY.GDP.MKTP.CD'):

        start_date = date_range.get("start", None)
        end_date = date_range.get("end", None)
        for location in locations:
            location_id = self.country_to_id_map.get(location, None)
            if location_id is None:
                continue
            URL_ind = 'https://api.worldbank.org/v2/countries/'+location_id+'/indicators/'+dataset_id+'?format=json&date='+start_date+':'+end_date
            response_ind = requests.get(url=URL_ind)
            json_respose_ind = json.loads(response_ind.content)
            pages_per_ind = json_respose_ind[0]['pages']
            all_data = []
            for i in range(1, pages_per_ind + 1):
                p = {'page': i}
                response_pagewise = requests.get(url=URL_ind, params=p)
                json_pagewise = json.loads(response_pagewise.content)
                all_data.extend(json_pagewise[1])
            df = pd.io.json.json_normalize(all_data)
            URL_ind_metadata = 'https://api.worldbank.org/v2/indicators/' + dataset_id + '?format=json'
            response_metadata = requests.get(url=URL_ind_metadata)
            json_respose_metadata = json.loads(response_metadata.content)
            json_respose_metadata = json_respose_metadata[1][0]
            sourceNote = json_respose_metadata['sourceNote']
            sourceOrganization = json_respose_metadata['sourceOrganization']
            df['sourceNote'] = sourceNote
            df['sourceOrganization'] = sourceOrganization
            return df





