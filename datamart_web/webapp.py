import os
import json
import sys
import pandas as pd
# When load spacy in a route, it will raise error. So do not remove "import spacy" here:
import spacy

sys.path.append(sys.path.append(os.path.join(os.path.dirname(__file__), '..')))

from flask import Flask, request
from datamart_web.src.search_metadata import SearchMetadata
from datamart_web.src.join_datasets import JoinDatasets

from datamart import search, augment, join, generate_metadata, upload, bulk_generate_metadata, bulk_upload
from datamart.utilities.utils import SEARCH_URL, ES_PORT, ES_HOST, PRODUCTION_ES_INDEX, Utils
from datamart.es_managers.query_manager import QueryManager

from datamart.data_loader import DataLoader


class WebApp(Flask):

    def __init__(self):
        super().__init__(__name__, instance_relative_config=True)
        self.search_metadata = SearchMetadata()
        self.join_datasets = JoinDatasets()
        self.old_df = None
        self.results = []

    @staticmethod
    def read_file(files, key, _type):
        if files and key in files:
            try:
                if _type == 'csv':
                    return pd.read_csv(files[key]).infer_objects()
                elif _type == 'json':
                    return json.load(files[key])
            except:
                pass

    def create_app(self, test_config=None):
        # create and configure the app
        self.config.from_mapping(
            SECRET_KEY='dev',
            DATABASE=os.path.join(self.instance_path, 'flaskr.sqlite'),
        )

        if test_config is None:
            # load the instance config, if it exists, when not testing
            self.config.from_pyfile('config.py', silent=True)
        else:
            # load the test config if passed in
            self.config.from_mapping(test_config)

        # ensure the instance folder exists
        try:
            os.makedirs(self.instance_path)
        except OSError:
            pass

        @self.route('/')
        def hello():
            return 'Datamart Web Service!'

        @self.route('/search/default_search', methods=['POST'])
        def default_search():
            print(request.files['file'])
            old_df = pd.read_csv(request.files['file']).infer_objects()
            if old_df is None or old_df.empty:
                return json.dumps({
                    "message": "Failed to create Dataframe from csv, nothing found"
                })
            self.old_df = old_df
            print('returning')
            return json.dumps(self.search_metadata.default_search_by_csv(request=request, old_df=self.old_df))

        @self.route('/augment/default_join', methods=['POST'])
        def default_join():
            if self.old_df is None or self.old_df.empty:
                return json.dumps({
                    "message": "Failed to join, no dataset uploaded"
                })
            return self.join_datasets.default_join(request=request, old_df=self.old_df)

        # ----- All APIs below are for the new APIs from 2019 winter workshop -----
        @self.route('/new/search_data', methods=['POST'])
        def search_data():
            try:
                query = self.read_file(request.files, 'query', 'json')
                data = self.read_file(request.files, 'data', 'csv')
                res = search(SEARCH_URL, query, data, max_return_docs=10)
                results = []
                for r in res:
                    cur = {
                        'summary': r.summary,
                        'score': r.score,
                        'metadata': r.metadata,
                        'datamart_id': r.id,
                    }
                    results.append(cur)
                return self.wrap_response(code='0000',
                                          msg='Success',
                                          data=results)
            except Exception as e:
                return self.wrap_response(code='1000', msg="FAIL - " + str(e))

        @self.route('/new/materialize_data', methods=['GET'])
        def materialize_data():
            try:
                datamart_id = int(request.args.get('datamart_id'))
                meta, df = DataLoader.load_meta_and_data_by_id(datamart_id)
                csv = df.to_csv(index=False)
                return self.wrap_response('0000', data=csv)
            except Exception as e:
                return self.wrap_response('1000', msg="FAIL - " + str(e))

        @self.route('/new/join_data', methods=['POST'])
        def join_data():
            try:
                left_df = self.read_file(request.files, 'left_data', 'csv')
                right_id = int(request.form.get('right_data'))
                left_columns = json.loads(request.form.get('left_columns'))
                right_columns = json.loads(request.form.get('right_columns'))
                joined_df = join(left_data=left_df,
                                 right_data=right_id,
                                 left_columns=left_columns,
                                 right_columns=right_columns)
                joined_csv = joined_df.to_csv(index=False)
                return self.wrap_response('0000', data=joined_csv)
            except Exception as e:
                return self.wrap_response('1000', msg="FAIL JOIN - " + str(e))


        return self

    @staticmethod
    def wrap_response(code, msg='', data=None):
        return json.dumps({
            'code': code,
            'message': msg or ('Success' if code == '0000' else 'Failed'),
            'data': data
        }, indent=2, default=lambda x: str(x))


if __name__ == '__main__':
    WebApp().create_app().run(host="0.0.0.0", port=9001, debug=False, ssl_context='adhoc')
