import pandas as pd
import os
import requests
import time
import json


class TwoRavensProfiler(object):
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

        temp_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'datamart_temp.csv')
        self.file_save(temp_path, inputs)
        f = dict(source_file=open(temp_path, 'rb'))
        raw_iter_times = 5
        callback_url = self.tr_raw_reader(f, raw_iter_times)
        if callback_url:
            time.sleep(1)
            json_iter_times = 5
            dataset, variables = self.tr_json_reader(callback_url, json_iter_times)
            if dataset or variables:
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

    def tr_raw_reader(self, file, raw_iter_times):
        if raw_iter_times > 0:
            raw_iter_times -= 1
            try:
                r = requests.post(self.tr_url, files=file, timeout=2)
                if r.status_code != 200:
                    print('Error: ', r.text)
                    time.sleep(1)
                    self.tr_raw_reader(file, raw_iter_times)
                else:
                    resp_json = r.json()
                    if resp_json['data']['state'] != 'FAILURE':
                        print('Raw json got!')
                        return resp_json['callback_url']
            except:
                print('Error')
                time.sleep(1)
                self.tr_raw_reader(file, raw_iter_times)
        else:
            return None

    def tr_json_reader(self, url, json_iter_times=5):
        if json_iter_times > 0:
            json_iter_times -= 1
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
                        self.tr_json_reader(url, json_iter_times)
            except:
                print('Error')
                time.sleep(1)
                self.tr_json_reader(url, json_iter_times)
        else:
            return None, None

    def file_save(self, file_path: str, csv_file: pd.DataFrame):
        self.file_delete(file_path)
        csv_file.to_csv(file_path, sep=',', index=False)

    def file_delete(self, file_path: str):
        exist = os.path.isfile(file_path)
        if exist:
            os.remove(file_path)