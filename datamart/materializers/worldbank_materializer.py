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


#DEFAULT_LOCATIONS = ["Aruba","Afghanistan","Africa","Angola","Albania","Andorra","Andean Region","Arab World","United Arab Emirates","Argentina","Armenia","American Samoa","Antigua and Barbuda","Australia","Austria","Azerbaijan","Burundi","East Asia & Pacific (IBRD-only countries)","Europe & Central Asia (IBRD-only countries)","Belgium","Benin","Burkina Faso","Bangladesh","Bulgaria","IBRD countries classified as high income","Bahrain","Bahamas, The","Bosnia and Herzegovina","Latin America & the Caribbean (IBRD-only countries)","Belarus","Belize","Middle East & North Africa (IBRD-only countries)","Bermuda","Bolivia","Brazil","Barbados","Brunei Darussalam","Sub-Saharan Africa (IBRD-only countries)","Bhutan","Botswana","Sub-Saharan Africa (IFC classification)","Central African Republic","Canada","East Asia and the Pacific (IFC classification)","Central Europe and the Baltics","Europe and Central Asia (IFC classification)","Switzerland","Channel Islands","Chile","China","Cote d'Ivoire","Latin America and the Caribbean (IFC classification)","Middle East and North Africa (IFC classification)","Cameroon","Congo, Dem. Rep.","Congo, Rep.","Colombia","Comoros","Cabo Verde","Costa Rica","South Asia (IFC classification)","Caribbean small states","Cuba","Curacao","Cayman Islands","Cyprus","Czech Republic","East Asia & Pacific (IDA-eligible countries)","Europe & Central Asia (IDA-eligible countries)","Germany","IDA countries classified as Fragile Situations","Djibouti","Latin America & the Caribbean (IDA-eligible countries)","Dominica","Middle East & North Africa (IDA-eligible countries)","IDA countries not classified as Fragile Situations","Denmark","IDA countries in Sub-Saharan Africa not classified as fragile situations ","Dominican Republic","South Asia (IDA-eligible countries)","IDA countries in Sub-Saharan Africa classified as fragile situations ","Sub-Saharan Africa (IDA-eligible countries)","IDA total, excluding Sub-Saharan Africa","Algeria","East Asia & Pacific (excluding high income)","Early-demographic dividend","East Asia & Pacific","Europe & Central Asia (excluding high income)","Europe & Central Asia","Ecuador","Egypt, Arab Rep.","Euro area","Eritrea","Spain","Estonia","Ethiopia","European Union","Fragile and conflict affected situations","Finland","Fiji","France","Faroe Islands","Micronesia, Fed. Sts.","IDA countries classified as fragile situations, excluding Sub-Saharan Africa","Gabon","United Kingdom","Georgia","Ghana","Gibraltar","Guinea","Gambia, The","Guinea-Bissau","Equatorial Guinea","Greece","Grenada","Greenland","Guatemala","Guam","Guyana","High income","Hong Kong SAR, China","Honduras","Heavily indebted poor countries (HIPC)","Croatia","Haiti","Hungary","IBRD, including blend","IBRD only","IDA & IBRD total","IDA total","IDA blend","Indonesia","IDA only","Isle of Man","India","Not classified","Ireland","Iran, Islamic Rep.","Iraq","Iceland","Israel","Italy","Jamaica","Jordan","Japan","Kazakhstan","Kenya","Kyrgyz Republic","Cambodia","Kiribati","St. Kitts and Nevis","Korea, Rep.","Kuwait","Latin America & Caribbean (excluding high income)","Lao PDR","Lebanon","Liberia","Libya","St. Lucia","Latin America & Caribbean ","Latin America and the Caribbean","Least developed countries: UN classification","Low income","Liechtenstein","Sri Lanka","Lower middle income","Low & middle income","Lesotho","Late-demographic dividend","Lithuania","Luxembourg","Latvia","Macao SAR, China","St. Martin (French part)","Morocco","Central America","Monaco","Moldova","Middle East (developing only)","Madagascar","Maldives","Middle East & North Africa","Mexico","Marshall Islands","Middle income","Macedonia, FYR","Mali","Malta","Myanmar","Middle East & North Africa (excluding high income)","Montenegro","Mongolia","Northern Mariana Islands","Mozambique","Mauritania","Mauritius","Malawi","Malaysia","North America","North Africa","Namibia","New Caledonia","Niger","Nigeria","Nicaragua","Netherlands","Non-resource rich Sub-Saharan Africa countries, of which landlocked","Norway","Nepal","Non-resource rich Sub-Saharan Africa countries","Nauru","IDA countries not classified as fragile situations, excluding Sub-Saharan Africa","New Zealand","OECD members","Oman","Other small states","Pakistan","Panama","Peru","Philippines","Palau","Papua New Guinea","Poland","Pre-demographic dividend","Puerto Rico","Korea, Dem. People’s Rep.","Portugal","Paraguay","West Bank and Gaza","Pacific island small states","Post-demographic dividend","French Polynesia","Qatar","Romania","Resource rich Sub-Saharan Africa countries","Resource rich Sub-Saharan Africa countries, of which oil exporters","Russian Federation","Rwanda","South Asia","Saudi Arabia","Southern Cone","Sudan","Senegal","Singapore","Solomon Islands","Sierra Leone","El Salvador","San Marino","Somalia","Serbia","Sub-Saharan Africa (excluding high income)","South Sudan","Sub-Saharan Africa ","Small states","Sao Tome and Principe","Suriname","Slovak Republic","Slovenia","Sweden","Eswatini","Sint Maarten (Dutch part)","Sub-Saharan Africa excluding South Africa","Seychelles","Syrian Arab Republic","Turks and Caicos Islands","Chad","East Asia & Pacific (IDA & IBRD countries)","Europe & Central Asia (IDA & IBRD countries)","Togo","Thailand","Tajikistan","Turkmenistan","Latin America & the Caribbean (IDA & IBRD countries)","Timor-Leste","Middle East & North Africa (IDA & IBRD countries)","Tonga","South Asia (IDA & IBRD)","Sub-Saharan Africa (IDA & IBRD countries)","Trinidad and Tobago","Tunisia","Turkey","Tuvalu","Taiwan, China","Tanzania","Uganda","Ukraine","Upper middle income","Uruguay","United States","Uzbekistan","St. Vincent and the Grenadines","Venezuela, RB","British Virgin Islands","Virgin Islands (U.S.)","Vietnam","Vanuatu","World","Samoa","Kosovo","Sub-Saharan Africa excluding South Africa and Nigeria","Yemen, Rep.","South Africa","Zambia","Zimbabwe"]

class WorldBankMaterializer(MaterializerBase):

    def __init__(self):
        MaterializerBase.__init__(self)
        self.headers = None
        resources_path = os.path.join(os.path.dirname(__file__), "../resources")
        with open(os.path.join(resources_path, 'country_to_id.json'), 'r') as json_file:
            reader = json.load(json_file)
            self.country_to_id_map = reader
            dict = self.country_to_id_map
            countrynames = dict.keys()
            self.DEFAULT_LOCATIONS = []
            for i in countrynames:
                self.DEFAULT_LOCATIONS.append(i)
            print (self.DEFAULT_LOCATIONS)

    def get(self, metadata: dict = None, variables: typing.List[int] = None, constrains: dict = None) -> pd.DataFrame:
        if not constrains:
            constrains = dict()
        date_range = constrains.get("date_range", {
            "start": (datetime.datetime.today() - datetime.timedelta(days=1 * 365)).strftime('%Y'),
            "end": datetime.datetime.today().strftime('%Y')})

        locations = constrains.get("locations", None)
        dataset_id = constrains.get("dataset_id", 'NY.GDP.MKTP.CD')
        return self.fetch_data(date_range=date_range, locations=locations,dataset_id=dataset_id)

    def fetch_data(self, date_range: dict = None, locations: list = None, dataset_id: str = 'NY.GDP.MKTP.CD'):

        start_date = date_range.get("start", None)
        end_date = date_range.get("end", None)
        if locations==None:
            locations=self.DEFAULT_LOCATIONS
        #print(locations)
        appended_data = []

        for location in locations:
            location_id = self.country_to_id_map.get(location, None)
            if location_id is None:
                continue
            URL_ind = 'https://api.worldbank.org/v2/countries/'+location_id+'/indicators/'+dataset_id+'?format=json&date='+start_date+':'+end_date
            response_ind = requests.get(url=URL_ind)
            json_respose_ind = json.loads(response_ind.content)
            #print(json_respose_ind)
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
            #globaldf.append(df)
        appended_data = pd.concat(appended_data, axis=0, sort=False)
        return appended_data




