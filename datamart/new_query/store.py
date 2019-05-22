import pandas as pd
import requests
import os, json, re, argparse
import string
import wikifier
import typing
from requests.auth import HTTPBasicAuth
from etk.etk import ETK
from etk.extractors.excel_extractor import ExcelExtractor
from etk.knowledge_graph import KGSchema, URI, Literal, LiteralType, Subject, Reification
from etk.etk_module import ETKModule
from etk.wikidata.entity import WDProperty, WDItem
from etk.wikidata.value import Datatype, Item, TimeValue, Precision, QuantityValue, StringValue, URLValue, MonolingualText
from etk.wikidata.statement import WDReference
from etk.wikidata import serialize_change_record
from etk.wikidata.truthy import TruthyUpdater
from SPARQLWrapper import SPARQLWrapper, JSON, POST, URLENCODED
from datamart.utilities.utils import Utils as datamart_utils
from wikifier import config
from io import StringIO

# WIKIDATA_QUERY_SERVER = config.endpoint_main
# WIKIDATA_UPDATE_SERVER = config.endpoint_update_main
WIKIDATA_QUERY_SERVER = config.endpoint_query_test  # this is testing wikidata
WIKIDATA_UPDATE_SERVER = config.endpoint_upload_test  # this is testing wikidata

class Datamart_dataset:
    def __init__(self):
        self.punctuation_table = str.maketrans(dict.fromkeys(string.punctuation))

        # initialize
        kg_schema = KGSchema()
        kg_schema.add_schema('@prefix : <http://isi.edu/> .', 'ttl')
        etk = ETK(kg_schema=kg_schema, modules=ETKModule)
        self.doc = etk.create_document({}, doc_id="http://isi.edu/default-ns/projects")

        # bind prefixes
        self.doc.kg.bind('wikibase', 'http://wikiba.se/ontology#')
        self.doc.kg.bind('wd', 'http://www.wikidata.org/entity/')
        self.doc.kg.bind('wdt', 'http://www.wikidata.org/prop/direct/')
        self.doc.kg.bind('wdtn', 'http://www.wikidata.org/prop/direct-normalized/')
        self.doc.kg.bind('wdno', 'http://www.wikidata.org/prop/novalue/')
        self.doc.kg.bind('wds', 'http://www.wikidata.org/entity/statement/')
        self.doc.kg.bind('wdv', 'http://www.wikidata.org/value/')
        self.doc.kg.bind('wdref', 'http://www.wikidata.org/reference/')
        self.doc.kg.bind('p', 'http://www.wikidata.org/prop/')
        self.doc.kg.bind('pr', 'http://www.wikidata.org/prop/reference/')
        self.doc.kg.bind('prv', 'http://www.wikidata.org/prop/reference/value/')
        self.doc.kg.bind('prn', 'http://www.wikidata.org/prop/reference/value-normalized/')
        self.doc.kg.bind('ps', 'http://www.wikidata.org/prop/statement/')
        self.doc.kg.bind('psv', 'http://www.wikidata.org/prop/statement/value/')
        self.doc.kg.bind('psn', 'http://www.wikidata.org/prop/statement/value-normalized/')
        self.doc.kg.bind('pq', 'http://www.wikidata.org/prop/qualifier/')
        self.doc.kg.bind('pqv', 'http://www.wikidata.org/prop/qualifier/value/')
        self.doc.kg.bind('pqn', 'http://www.wikidata.org/prop/qualifier/value-normalized/')
        self.doc.kg.bind('skos', 'http://www.w3.org/2004/02/skos/core#')
        self.doc.kg.bind('prov', 'http://www.w3.org/ns/prov#')
        self.doc.kg.bind('schema', 'http://schema.org/')

        # give definition of the nodes we definied
        p = WDProperty('C2001', Datatype.MonolingualText)
        p.add_label('keywords', lang='en')
        p.add_description('identifier of a dataset in the Datamart system', lang='en')
        p.add_statement('P31', Item('Q19847637'))
        p.add_statement('P1629', Item('Q1172284'))
        self.doc.kg.add_subject(p)

        p = WDProperty('C2004', Datatype.StringValue)
        p.add_label('datamart identifier', lang='en')
        p.add_description('keywords associated with an item to facilitate finding the item using text search', lang='en')
        p.add_statement('P31', Item('Q18616576'))
        self.doc.kg.add_subject(p)

        p = WDProperty('C2005', Datatype.StringValue)
        p.add_label('variable measured', lang='en')
        p.add_description('the variables measured in a dataset', lang='en')
        p.add_statement('P31', Item('Q18616576'))
        p.add_statement('P1628', URLValue('http://schema.org/variableMeasured'))
        self.doc.kg.add_subject(p)

        p = WDProperty('C2006', Datatype.StringValue)
        p.add_label('values', lang='en')
        p.add_description('the values of a variable represented as a text document', lang='en')
        p.add_statement('P31', Item('Q18616576'))
        self.doc.kg.add_subject(p)

        p = WDProperty('C2007', Datatype.Item)
        p.add_label('data type', lang='en')
        p.add_description('the data type used to represent the values of a variable, integer (Q729138), Boolean (Q520777), Real (Q4385701), String (Q184754), Categorical (Q2285707)', lang='en')
        p.add_statement('P31', Item('Q18616576'))
        self.doc.kg.add_subject(p)

        p = WDProperty('C2008', Datatype.URLValue)
        p.add_label('semantic type', lang='en')
        p.add_description('a URL that identifies the semantic type of a variable in a dataset', lang='en')
        p.add_statement('P31', Item('Q18616576'))
        self.doc.kg.add_subject(p)

        # get the starting source id
        sparql_query = """select ?x where {
                        wd:Z00000 wdt:P1114 ?x .
                       }"""
        try:
            sparql = SPARQLWrapper(WIKIDATA_QUERY_SERVER)
            sparql.setQuery(sparql_query)
            sparql.setReturnFormat(JSON)
            sparql.setMethod(POST)
            sparql.setRequestMethod(URLENCODED)
            results = sparql.query().convert()['results']['bindings']
        except:
            print("Getting query of wiki data failed!")
            raise ValueError("Unable to initialize the datamart query service")
        if not results:
            print("[WARNING] No starting source id found! Will initialize the starting source with D1000001")
            self.resource_id = 1000001
        else:
            self.resource_id = 1000001

    def load_and_preprocess(self, input_dir, file_type="csv"):
        if file_type=="csv":
            try:
                loaded_data = pd.read_csv(input_dir)
            except:
                raise ValueError("Reading csv from" + input_dir + "failed.")
        # elif file_type=="":
        else:
            raise ValueError("Unsupported file type")

        loaded_data = loaded_data.fillna("")
        # TODO: need update profiler here to generate better semantic type
        metadata = datamart_utils.generate_metadata_from_dataframe(data=loaded_data)
        self.columns_are_string = []
        for i, each_column_meta in enumerate(metadata['variables']):
            if 'http://schema.org/Text' in each_column_meta['semantic_type']:
                self.columns_are_string.append(i)
        wikifier_res = wikifier.produce(loaded_data, target_columns=self.columns_are_string)
        # add Q nodes part
        for i in range(loaded_data.shape[1], wikifier_res.shape[1]):
            self.columns_are_string.append(i)
        return wikifier_res, metadata

    def model_data(self, input_df:pd.DataFrame, metadata: dict):
        if metadata is None:
            metadata = {}
        title = metadata.get("title") or ""
        keywords = metadata.get("keywords") or ""
        url = metadata.get("url") or "https://"
        if type(keywords) is list:
            keywords = " ".join(keywords)
        node_id = 'D' + str(self.resource_id)
        q = WDItem(node_id)
        self.resource_id += 1
        q.add_label('datasets ' + node_id, lang='en')
        q.add_statement('P31', Item('Q1172284'))  # indicate it is subclass of a dataset
        q.add_statement('P2699', URLValue(url))  # url
        q.add_statement('P1476', MonolingualText(title, lang='en'))  # title
        q.add_statement('C2001', StringValue(node_id))  # datamart identifier
        q.add_statement('C2004', StringValue(keywords))  # keywords
        # each columns
        for i in self.columns_are_string:
            try: 
                semantic_type = metadata['variables'][i]['semantic_type']
            except IndexError:
                semantic_type = 'http://schema.org/Text'
            res = self.process_one_column(column_data=input_df.iloc[:,i], item=q, column_number=i, semantic_type=semantic_type)
            if not res:
                print("Error when adding column " + str(i))
        self.doc.kg.add_subject(q)

    def process_one_column(self, column_data: pd.Series, item: WDItem, column_number: int, semantic_type: typing.List[str]) -> bool:
        """
        :param column_data: a pandas series data
        :param item: the target q node aimed to add on
        :param column_number: the column number
        :param semantic_type: a list indicate the semantic tpye of this column
        :return: a bool indicate succeeded or not
        """
        try:
            all_data = set(column_data.tolist())
            all_value_str_set = set()
            for each in all_data:
                # set to lower characters, remove punctuation and split by the space
                words_processed = str(each).lower().translate(self.punctuation_table).split()
                for word in words_processed:
                    all_value_str_set.add(word)
            all_value_str = " ".join(all_value_str_set)

            statement = item.add_statement('C2005', StringValue(column_data.name))  # variable measured
            statement.add_qualifier('C2006', StringValue(all_value_str))  # values
            if 'http://schema.org/Float' in semantic_type:
                semantic_type_url = 'http://schema.org/Float'
                data_type = "float"
            elif 'http://schema.org/Integer' in semantic_type:
                data_type = "int"
                semantic_type_url = 'http://schema.org/Integer'
            elif 'http://schema.org/Text' in semantic_type:
                data_type = "string"
                semantic_type_url = 'http://schema.org/Text'

            statement.add_qualifier('C2007', Item(data_type))  # data structure type
            statement.add_qualifier('C2008', URLValue(semantic_type_url))  # semantic type identifier
            statement.add_qualifier('P1545', QuantityValue(column_number))  # column index
            return True
        except:
            # import pdb
            # pdb.set_trace()
            return False

    def output_to_ttl(self, file_path:                      str, file_format="ttl"):                        
        """
            output the file only but not upload
        """
        f = open(file_path + ".ttl", 'w')
        f.write(self.doc.kg.serialize(file_format))
        f.close()
        with open(file_path + 'changes.tsv', 'w') as fp:
            serialize_change_record(fp)
        print('Serialization completed!')

    def upload(self):
        """
            upload the dataset
        """
        # This special Q node is used to store the next count to store the new Q node
        sparql_query = """delete {
              wd:Z00000 wdt:P1114 ?x .
            }
            where {
                wd:Z00000 wdt:P1114 ?x .
            }
        """
        try:
            sparql = SPARQLWrapper(WIKIDATA_UPDATE_SERVER)
            sparql.setQuery(sparql_query)
            sparql.setReturnFormat(JSON)
            sparql.setMethod(POST)
            sparql.setRequestMethod(URLENCODED)
            sparql.setCredentials(config.user, config.password)
            results = sparql.query()  #.convert()['results']['bindings']
        except:
            print("Updating the count for datamart failed!")
            raise ValueError("Unable to connect to datamart query service")
        # add datamart count to ttl
        q = WDItem('Z00000')
        q.add_label('Datamart datasets count', lang='en')
        q.add_statement('P1114', QuantityValue(self.resource_id))  # title
        self.doc.kg.add_subject(q)
        # upload
        extracted_data = self.doc.kg.serialize("ttl")
        headers = {'Content-Type': 'application/x-turtle',}
        response = requests.post(WIKIDATA_UPDATE_SERVER, data=extracted_data, headers=headers,
                                 auth=HTTPBasicAuth(config.user, config.password))
        print('Upload file finished with status code: {}!'.format(response.status_code))
        temp_output = StringIO()
        serialize_change_record(temp_output)
        tu = TruthyUpdater(WIKIDATA_UPDATE_SERVER, False, config.user, config.password)
        np_list = []

        for l in temp_output.readlines():
            if not l: continue
            node, prop = l.strip().split('\t')
            np_list.append((node, prop))
        tu.build_truthy(np_list)

        print('Update truthy finished!')
