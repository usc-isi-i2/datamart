from datamart.materializers.materializer_base import MaterializerBase
import datetime
import requests
import typing
import os
import json
import pandas as pd
from pandas.io.json import json_normalize


class WorldBankMaterializer(MaterializerBase):

    def __init__(self):
        MaterializerBase.__init__(self)
        self.headers = None
        resources_path = os.path.join(os.path.dirname(__file__), "../resources")
        with open(os.path.join(resources_path, 'country_to_id.json'), 'r') as json_file:
            reader = json.load(json_file)
            self.country_to_id_map = reader
            countrynames = self.country_to_id_map.keys()
            self.DEFAULT_LOCATIONS = list(countrynames)

    def get(self, metadata: dict = None, variables: typing.List[int] = None, constrains: dict = None) -> pd.DataFrame:
        if not constrains:
            constrains = dict()
        date_range = constrains.get("date_range", {
            "start": (datetime.datetime.today() - datetime.timedelta(days=1 * 365)).strftime('%Y'),
            "end": datetime.datetime.today().strftime('%Y')})

        locations = constrains.get("named_entity", None)
        dataset_id = constrains.get("dataset_id", 'NY.GDP.MKTP.CD')
        return self.fetch_data(date_range=date_range, locations=locations, dataset_id=dataset_id)

    def fetch_data(self, date_range: dict = None, locations: list = None, dataset_id: str = 'NY.GDP.MKTP.CD'):

        start_date = date_range.get("start", None)
        end_date = date_range.get("end", None)
        if not locations:
            locations = self.DEFAULT_LOCATIONS

        appended_data = []

        for location in locations:
            location_id = self.country_to_id_map.get(location, None)
            if location_id is None:
                continue
            URL_ind = 'https://api.worldbank.org/v2/countries/' + location_id + '/indicators/' + dataset_id + '?format=json&date=' + start_date + ':' + end_date
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
            appended_data.append(df)
            # globaldf.append(df)
        appended_data = pd.concat(appended_data, axis=0, sort=False)
        return appended_data
