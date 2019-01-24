import pandas as pd
import os
import requests
import time
import json


class TwoRavenProfiler(object):
    def __init__(self):
        self.tr_url = 'http://metadata.2ravens.org/preprocess/api/process-single-file'
        pass

    def profile(self, inputs: pd.DataFrame, metadata: dict) -> dict:
        """Save metadata json to file.

        Args:
            inputs: pandas dataframe
            metadata: dict

        Returns:
            dict
        """

        temp_path = 'datamart_temp.csv'
        self.file_save(temp_path, inputs)
        f = dict(source_file=open(temp_path, 'rb'))
        callback_url = self.tr_raw_reader(f)
        time.sleep(1)
        dataset, variables = self.tr_json_reader(callback_url)
        metadata['two_ravens_dataset'] = dataset
        if 'variables' in metadata:
            self.add_variables(variables, metadata['variables'])
        self.file_delete(temp_path)
        return metadata

    def add_variables(self, variables, metadata: list):
        for v in variables:
            for m in metadata:
                if v == m['name']:
                    m['two_ravens_variables'] = variables[v]

    def tr_raw_reader(self, file):
        try:
            r = requests.post(self.tr_url, files=file, timeout=2)
            if r.status_code != 200:
                print('Error: ', r.text)
                time.sleep(1)
                self.tr_raw_reader(file)
            else:
                resp_json = r.json()
                if resp_json['data']['state'] != 'FAILURE':
                    print('Raw json got!')
                    return resp_json['callback_url']
        except:
            print('Error')
            time.sleep(1)
            self.tr_raw_reader(file)

    def tr_json_reader(self, url):
        try:
            f = requests.get(url, timeout=3)
            if f.status_code == 200:
                content_json = json.loads(f.text)
                print('State: ', content_json['data']['state'])
                if content_json['data']['state'] == 'SUCCESS':
                    dataset = content_json['data']['summary_metadata']['dataset']
                    variables = content_json['data']['summary_metadata']['variables']
                    print('Two Ravens data got!')
                    return dataset, variables
                else:
                    time.sleep(1)
                    self.tr_json_reader(url)
        except:
            print('Error')
            time.sleep(1)
            self.tr_json_reader(url)

    def file_save(self, file_path: str, csv_file: pd.DataFrame):
        self.file_delete(file_path)
        csv_file.to_csv(file_path, sep=',', index=False)

    def file_delete(self, file_path: str):
        exist = os.path.isfile(file_path)
        if exist:
            os.remove(file_path)