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

from datamart.entries import search, upload


class WebApp(Flask):

    def __init__(self):
        super().__init__(__name__, instance_relative_config=True)
        self.search_metadata = SearchMetadata()
        self.join_datasets = JoinDatasets()
        self.old_df = None
        self.results = []

    @staticmethod
    def read_file(files, key, _type):
        if key in files:
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

        @self.route('/new/upload_data', methods=['POST'])
        def upload_data():
            try:
                description = self.read_file(request.files, 'file', 'json')
                es_index = 'datamart_tmp' if request.form.get('test') == 'true' else 'datamart_all'
                print(description, es_index)
                res = upload(description, es_index)
                return self.wrap_response('0000', data=res)
            except Exception as e:
                return self.wrap_response('1000', msg="FAIL - " + str(e))

        @self.route('/new/search_data', methods=['POST'])
        def search_data():
            self.results = []
            try:
                query = self.read_file(request.files, 'query', 'json')
                data = self.read_file(request.files, 'data', 'csv')
                res = search(query, data)
                self.results = res
                return self.wrap_response('0000', data=[r._es_raw_object for r in res])
            except Exception as e:
                return self.wrap_response('1000', msg="FAIL - " + str(e))

        @self.route('/new/materialize_data', methods=['GET'])
        def materialize_data():
            try:
                index = int(request.args.get('index'))
                dataset = self.results[index]
                return self.wrap_response('0000', data=dataset.materialize().to_csv(index=False))
            except Exception as e:
                return self.wrap_response('1000', msg="FAIL - " + str(e))

        @self.route('/temp_gui', methods=['GET'])
        def temp_gui():
            return '''
<div>
    <h3> Upload </h3>
    <form id="uploadbanner" enctype="multipart/form-data" method="post" action="/new/upload_data">
        <ul>
            <li>
                <p>Please upload a description json for your dataset
                    <a href="https://datadrivendiscovery.org/wiki/display/work/Datamart+user+upload+dataset+API">(see explain)</a>:
                </p>
                <input name="file" type="file" />
                <br />
            </li>
            <li>
                <p>If checked, the data will not be uploaded to the in-use endpoint but the one for test: </p>
                <input name="test" type="checkbox" value="datamart_tmp" checked>Just for test</input>
                <br />
            </li>
            <li>
                <p>When submitted, please wait for a while until you got a json response with success/fail message and the metadata put in Datamart:</p>
                <input type="submit" value="submit" id="submit" />
            </li>
        </ul>
    </form>
    
    <br />
    <br />
    
    <h3> Search </h3>
    <form id="searchbanner" enctype="multipart/form-data" method="post" action="/new/search_data">
        <ul>
            <li>
                <p>Please upload a description json for your target datasets
                    <a href="https://datadrivendiscovery.org/wiki/display/work/Query+input+samples">(see examples)</a>:
                </p>
                <input name="query" type="file" />
                <br />
            </li>
            <li>
                <p>
                    Please upload the data(csv file) you would like to argument:
                </p>
                <input name="data" type="file" />
                <br />
            </li>
            <li>
                <input type="submit" value="submit" id="submit" />
                <br />It will return the search results(a list of dataset description documents). 
            </li>
        </ul>
    </form>
</div>
'''

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
