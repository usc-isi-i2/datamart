import os
import json
import sys
import pandas as pd
# When load spacy in a route, it will raise error. So do not remove "import spacy" here:
import spacy
import traceback

sys.path.append(sys.path.append(os.path.join(os.path.dirname(__file__), '..')))

from flask import Flask, request, render_template
from datamart_web.src.search_metadata import SearchMetadata
from datamart_web.src.join_datasets import JoinDatasets

from datamart import search, join, generate_metadata, upload, bulk_generate_metadata, bulk_upload
from datamart.utilities.utils import SEARCH_URL, PRODUCTION_ES_INDEX, TEST_ES_INDEX

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
            return 'Datamart Web Service!<a href="/gui">gui</a>'

        @self.route('/new/search_data', methods=['POST'])
        def search_data():
            try:
                query = self.read_file(request.files, 'query', 'json')
                data = self.read_file(request.files, 'data', 'csv')
                if not query and request.form.get('query_json'):
                    query = json.loads(request.form.get('query_json'))
                max_return_docs = int(request.args.get('max_return_docs')) if request.args.get('max_return_docs') else 10
                return_named_entity = False
                if request.args.get('return_named_entity') and request.args.get('return_named_entity').lower() != "false":
                    return_named_entity = True
                res = search(SEARCH_URL, query, data, max_return_docs=max_return_docs, return_named_entity=return_named_entity)
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
                return self.wrap_response(code='1000', msg="FAIL SEARCH - %s \n %s" %(str(e), str(traceback.format_exc())))

        @self.route('/new/materialize_data', methods=['GET'])
        def materialize_data():
            try:
                datamart_id = int(request.args.get('datamart_id'))
                first_n_rows = None
                try:
                    first_n_rows = int(request.args.get('first_n_rows'))
                except:
                    pass
                meta, df = DataLoader.load_meta_and_data_by_id(datamart_id, first_n_rows)
                csv = df.to_csv(index=False)
                return self.wrap_response('0000', data=csv)
            except Exception as e:
                return self.wrap_response('1000', msg="FAIL MATERIALIZE - %s \n %s" %(str(e), str(traceback.format_exc())))

        @self.route('/new/join_data', methods=['POST'])
        def join_data():
            try:
                left_df = self.read_file(request.files, 'left_data', 'csv')
                right_id = int(request.form.get('right_data'))
                left_columns = json.loads(request.form.get('left_columns'))
                right_columns = json.loads(request.form.get('right_columns'))
                exact_match = request.form.get('exact_match')
                if exact_match and exact_match.lower() == 'true':
                    joiner = 'default'
                else:
                    joiner = 'rltk'
                join_res = join(left_data=left_df,
                                 right_data=right_id,
                                 left_columns=left_columns,
                                 right_columns=right_columns,
                                 joiner=joiner)
                if join_res.df is not None:
                    joined_csv = join_res.df.to_csv(index=False)
                    return self.wrap_response('0000', data=joined_csv,
                                              matched_rows=join_res.matched_rows,
                                              cover_ratio=join_res.cover_ratio)
                else:
                    return self.wrap_response('2000', msg="Failed, invalid inputs")
            except Exception as e:
                return self.wrap_response('1000', msg="FAIL JOIN - %s \n %s" %(str(e), str(traceback.format_exc())))

        @self.route('/new/get_metadata_single_file', methods=['POST'])
        def get_metadata_single_file():
            try:
                description = request.json
                enable_two_ravens_profiler = False
                if request.args.get('enable_two_ravens_profiler') and request.args.get(
                        'enable_two_ravens_profiler').lower() != "false":
                    enable_two_ravens_profiler = True
                metadata_list = generate_metadata(description, enable_two_ravens_profiler=enable_two_ravens_profiler)
                return self.wrap_response('0000', data=metadata_list)
            except Exception as e:
                return self.wrap_response('1000', msg="FAIL GENERATE DATA FOR SINGLE FILE - %s \n %s" %(str(e), str(traceback.format_exc())))

        @self.route('/new/get_metadata_extract_links', methods=['POST'])
        def get_metadata_extract_links():
            try:
                enable_two_ravens_profiler = False
                if request.args.get('enable_two_ravens_profiler') and request.args.get(
                        'enable_two_ravens_profiler').lower() != "false":
                    enable_two_ravens_profiler = True
                url = request.json.get('url')
                description = request.json.get('description')
                metadata_lists = bulk_generate_metadata(html_page=url, description=description,
                                                        enable_two_ravens_profiler=enable_two_ravens_profiler)
                return self.wrap_response('0000', data=metadata_lists)
            except Exception as e:
                return self.wrap_response('1000', msg="FAIL GENERATE META FROM LINKS - %s \n %s" %(str(e), str(traceback.format_exc())))

        @self.route('/new/upload_metadata_list', methods=['POST'])
        def upload_list_of_metadata():
            try:
                all_metadata = request.json.get('metadata')
                for_test = request.json.get('for_test')
                allow_duplicates = request.json.get('allow_duplicates')
                es_index = TEST_ES_INDEX if for_test else PRODUCTION_ES_INDEX
                deduplicate = not allow_duplicates
                succeed = []
                if isinstance(all_metadata, dict):
                    succeed = upload(meta_list=[all_metadata],
                           es_index=es_index,
                           deduplicate=deduplicate)
                elif all_metadata and isinstance(all_metadata[0], dict):
                    succeed = upload(meta_list=all_metadata,
                           es_index=es_index,
                           deduplicate=deduplicate)
                elif all_metadata and isinstance(all_metadata[0], list):
                    succeed = bulk_upload(list_of_meta_list=all_metadata,
                                es_index=es_index,
                                deduplicate=deduplicate)
                return self.wrap_response('0000', data=succeed)
            except Exception as e:
                return self.wrap_response('1000', msg="FAIL UPLOAD - %s \n %s" %(str(e), str(traceback.format_exc())))

        # ----- gui for upload -----
        @self.route('/gui', methods=['GET'])
        def gui_index():
            return render_template("index.html")

        # def get_metadata_extract_links():
        #     try:
        #         url = request.json.get('url')
        #         description = request.json.get('description')
        #         metadata_lists = bulk_generate_metadata(html_page=url, description=description)
        #         return self.wrap_response('0000', data=metadata_lists)
        #     except Exception as e:
        #         return self.wrap_response('1000', msg="FAIL METADATA GENERATION - " + str(e))

        return self

    @staticmethod
    def wrap_response(code, msg='', data=None, **kwargs):
        return json.dumps({
            'code': code,
            'message': msg or ('Success' if code == '0000' else 'Failed'),
            'data': data,
            **kwargs
        }, indent=2, default=lambda x: str(x))


if __name__ == '__main__':
    WebApp().create_app().run(host="0.0.0.0", port=9000, debug=False, ssl_context=('cert.pem', 'key.pem'))
