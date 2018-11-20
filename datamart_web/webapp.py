import os
import json
import sys
import pandas as pd

sys.path.append(sys.path.append(os.path.join(os.path.dirname(__file__), '..')))

from flask import Flask, request
from datamart_web.src.search_metadata import SearchMetadata
from datamart_web.src.join_datasets import JoinDatasets


class WebApp(Flask):

    def __init__(self):
        super().__init__(__name__, instance_relative_config=True)
        self.search_metadata = SearchMetadata()
        self.join_datasets = JoinDatasets()
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
            old_df = pd.read_csv(request.files['file']).infer_objects()
            if old_df is None or old_df.empty:
                return json.dumps({
                    "message": "Failed to create Dataframe from csv, nothing found"
                })
            self.old_df = old_df
            return json.dumps(self.search_metadata.default_search_by_csv(request=request, old_df=self.old_df))

        @self.route('/augment/default_join', methods=['POST'])
        def default_join():
            if self.old_df is None or self.old_df.empty:
                return json.dumps({
                    "message": "Failed to join, no dataset uploaded"
                })
            return json.dumps(self.join_datasets.default_join(request=request, old_df=self.old_df))

        return self


if __name__ == '__main__':
    WebApp().create_app().run()
