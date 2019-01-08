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
from datamart_web.src.contribute_data import UserDataIndexer


class WebApp(Flask):

    def __init__(self):
        super().__init__(__name__, instance_relative_config=True)
        self.search_metadata = SearchMetadata()
        self.join_datasets = JoinDatasets()
        self.indexer = UserDataIndexer()
        self.old_df = None

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

        @self.route('/index/contribute_data', methods=['POST'])
        def contribute_data():
            try:
                file_content = request.files['file'].read().decode('utf-8')
                description = json.loads(file_content)
                es_index = request.form.get('es_index') or 'datamart_tmp'
                res = self.indexer.index(description, es_index)
                return self.wrap_response('0000', data=res)
            except Exception as e:
                return self.wrap_response('1000', msg=str(e))

        @self.route('/add_data_gui', methods=['GET'])
        def temp_gui():
            return '''<form id="uploadbanner" enctype="multipart/form-data" method="post" action="/index/contribute_data">
   <input name="file" type="file" />
   <input name="es_index" type="radio" value="datamart_tmp">datamart_tmp(for test)</input>
   <input name="es_index" type="radio" value="datamart_all">datamart_all(in use)</input>
   <input type="submit" value="submit" id="submit" />
   Will return the metadata doc sent to ES.
</form>'''

        return self

    @staticmethod
    def wrap_response(code, msg='', data=None):
        return json.dumps({
            'code': code,
            'message': msg or ('Success' if code == '0000' else 'Failed'),
            'data': data
        })


if __name__ == '__main__':
    WebApp().create_app().run(host="0.0.0.0", port=9000, debug=False)
