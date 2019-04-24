import pandas as pd
# import os
import copy
import frozendict
import collections
import typing
from d3m.container import DataFrame as d3m_DataFrame
from d3m.container import Dataset as d3m_Dataset
import d3m.metadata.base as metadata_base
from datamart.dataset import Dataset
from datamart.utilities.utils import PRODUCTION_ES_INDEX, SEARCH_URL
from datamart.es_managers.json_query_manager import JSONQueryManager
from datamart.augment import Augment
from datamart.data_loader import DataLoader
from d3m.base import utils as d3m_utils
from datamart.utilities.utils import Utils
from datamart.joiners.join_result import JoinResult
from datamart.joiners.joiner_base import JoinerType
from itertools import chain
from datamart.joiners.rltk_joiner import RLTKJoiner
from SPARQLWrapper import SPARQLWrapper, JSON
from d3m.metadata.base import ALL_ELEMENTS
from datamart.joiners.rltk_joiner import RLTKJoiner_new
import requests
import traceback
import logging

Q_NODE_SEMANTIC_TYPE = "http://wikidata.org/qnode"
DEFAULT_URL = "https://isi-datamart.edu"
__ALL__ = ('D3MDataMart', 'DataMartSearchResult', '')
SEARCH_RESULT = typing.TypeVar('RESULT', bound='DataMartSearchResult')
JOIN_SPEC = typing.TypeVar('D3MJoinSpec', bound='DataMartSearchResult')


class D3MDataMart:
    """
    ISI implementation of D3MDataMart
    """
    def __init__(self):
        self.url = DEFAULT_URL
        self._logger = logging.getLogger(__name__)

    def search(self, query=None, supplied_data: d3m_Dataset=None, timeout=None, limit: int=20) \
            -> typing.List[SEARCH_RESULT]:
        """
        :param query: JSON object describing the query.
        :param supplied_data: the data you are trying to augment.
        :param timeout: allowed time spent on searching
        :param limit: the limitation on the return amount of DataMartSearchResult
        :return: list of search results of DataMartSearchResult
        """
        if not self.url.startswith(SEARCH_URL):
            return []

        res_id, suppied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None)
        if query is None:
            can_query_columns = []
            for each in range(len(suppied_dataframe.columns)):
                selector = (res_id, ALL_ELEMENTS, each)
                each_column_meta = supplied_data.metadata.query(selector)
                if 'http://schema.org/Text' in each_column_meta["semantic_types"] or \
                        "https://metadata.datadrivendiscovery.org/types/CategoricalData" in each_column_meta[
                    "semantic_types"]:
                    can_query_columns.append(each)

            if len(can_query_columns) == 0:
                self._logger.error("No columns can be augment!")
                return supplied_data

            import pdb
            pdb.set_trace()
            for each_column in can_query_columns:
                column_values = suppied_dataframe.iloc[:, each_column]
                query_column_entities = list(set(column_values.tolist()))
                for i, each in enumerate(query_column_entities):
                    if "'" in each:
                        query_column_entities.remove(each)

                query_column_entities = query_column_entities[:100]

                search_query ={
                    "required_variables": [
                        {
                            "type": "generic_entity",
                            "named_entities":query_column_entities
                        }
                    ]
                }
                pdb.set_trace()
                result = self.search_general(query=search_query, supplied_data=suppied_dataframe)

        search_results = []

        # first take a search on wikidata
        wikidata_search_results = self.search_wiki_data(query, supplied_data, timeout)
        search_results.extend(wikidata_search_results)
        limit_remianed = limit - len(wikidata_search_results)
        # if not reach limit, continue search on general datasets
        if limit_remianed > 0:
            general_search_results = self.search_general(query, supplied_data, timeout, limit=limit_remianed)
            search_results.extend(general_search_results)

        return search_results

    def search_wiki_data(self,query, supplied_data: d3m_DataFrame=None, timeout=None, search_threshold=0.5) \
            -> typing.List[SEARCH_RESULT]:
        """
        The search function used for wikidata search
        :param query: JSON object describing the query.
        :param supplied_data: the data you are trying to augment.
        :param timeout: allowed time spent on searching
        :param limit: the limitation on the return amount of DataMartSearchResult
        :param search_threshold: the minimum appeared times of the properties
        :return: list of search results of DataMartSearchResult
        """
        wikidata_results = []
        try:
            q_nodes_columns = []
            metadata_input = supplied_data.metadata
            # check whether Qnode is given in the inputs, if given, use this to wikidata and search
            required_variables_names = None
            if 'required_variables' in query:
                required_variables_names = []
                for each in query['required_variables']:
                    required_variables_names.extend(each['names'])
            for i in range(supplied_data.shape[1]):
                metadata_selector = (metadata_base.ALL_ELEMENTS, i)
                if Q_NODE_SEMANTIC_TYPE in metadata_input.query(metadata_selector)["semantic_types"]:
                    # if no required variables given, attach any Q nodes found
                    if not required_variables_names:
                        q_nodes_columns.append(i)
                    # otherwise this column has to be inside required_variables
                    else:
                        if supplied_data.columns[i] in required_variables_names:
                            q_nodes_columns.append(i)

            if len(q_nodes_columns) == 0:
                print("No wikidata Q nodes detected on corresponding required_variables! Will skip wikidata search part")
                return wikidata_results
            else:

                print("Wikidata Q nodes inputs detected! Will search with it.")
                print("Totally " + str(len(q_nodes_columns)) + " Q nodes columns detected!")
                # do a wikidata search for each Q nodes column
                for each_column in q_nodes_columns:
                    q_nodes_list = supplied_data.iloc[:, each_column].tolist()
                    http_address = 'http://minds03.isi.edu:4444/get_properties'
                    headers = {"Content-Type": "application/json"}
                    requests_data = str(q_nodes_list)
                    requests_data = requests_data.replace("'", '"')
                    r = requests.post(http_address, data=requests_data, headers=headers)

                    p_count = collections.defaultdict(int)
                    p_nodes_needed = []
                    results = r.json()
                    for each_p_list in results.values():
                        for each_p in each_p_list:
                            p_count[each_p] += 1

                    for key, val in p_count.items():
                        if float(val) / len(q_nodes_list) >= search_threshold:
                            p_nodes_needed.append(key)
                    wikidata_search_result = {"p_nodes_needed": p_nodes_needed,
                                              "target_q_node_column_name": supplied_data.columns[each_column]}
                    wikidata_results.append(DataMartSearchResult(search_result=wikidata_search_result,
                                                                 supplied_data=supplied_data,
                                                                 query_json=query,
                                                                 search_type="wikidata")
                                            )
            return wikidata_results

        except:
            print("Searhcing with wiki data failed")
            traceback.print_exc()
        finally:
            return wikidata_results

    def search_general(self, query, supplied_data: d3m_DataFrame=None, timeout=None, limit: int=20) \
            -> typing.List[SEARCH_RESULT]:
        """
        The search function used for general elastic search
        :param query: JSON object describing the query.
        :param supplied_data: the data you are trying to augment.
        :param timeout: allowed time spent on searching
        :param limit: the limitation on the return amount of DataMartSearchResult
        :return: list of search results of DataMartSearchResult
        """
        augmenter = Augment(es_index=PRODUCTION_ES_INDEX)
        es_results = []
        try:
            if (query and ('required_variables' in query)) or (supplied_data is None):
                # if ("required_variables" exists or no data):
                cur_results = augmenter.query_by_json(query, supplied_data, size=limit) or []
                for res in cur_results:
                    es_results.append(DataMartSearchResult(search_result=res, supplied_data=supplied_data,
                                                           query_json=query, search_type="general")
                                      )

            else:
                # if there is no "required_variables" in the query JSON, but the supplied_data exists,
                # try each named entity column as "required_variables" and concat the results:
                query = query or {}
                exist = set()
                for col in supplied_data:
                    if Utils.is_column_able_to_query(supplied_data[col]):
                        # update 2019.4.9: we should not replace the original query!!!
                        query_copy = query.copy()
                        query_copy['required_variables'] = [{
                            "type": "dataframe_columns",
                            "names": [col]
                        }]
                        cur_results = augmenter.query_by_json(query_copy, supplied_data, size=limit)
                        if not cur_results:
                            continue
                        for res in cur_results:
                            if res['_id'] not in exist:
                                # TODO: how about the score ??
                                exist.add(res['_id'])
                            es_results.append(DataMartSearchResult(search_result=res, supplied_data=supplied_data,
                                                                   query_json=query_copy, search_type="general")
                                              )
            return es_results
        except:
            print("Searhcing with wiki data failed")
            traceback.print_exc()
        finally:
            return es_results

class DataMartSearchResult:
    """
    The class used to store each search results and corresponding search config of DataMart
    """
    def __init__(self, search_result, supplied_data, query_json, search_type):
        self.search_result = search_result
        if "_score" in self.search_result:
            self.score = self.search_result["_score"]
        if "_source" in self.search_result:
            self.metadata = self.search_result["_source"]
        self.supplied_data = supplied_data
        self.query_json = query_json
        self.search_type = search_type
        self.pairs = None
        self._res_id = None # only used for input is Dataset

    def download(self, supplied_data: d3m_DataFrame=None, generate_metadata=True) -> d3m_DataFrame:
        """
        download the dataFrame and corresponding metadata information of search result
        everytime call download, the DataFrame will have the exact same columns in same order
        """
        if self.search_type=="general":
            return_df = self.download_general(supplied_data, generate_metadata)
        elif self.search_type=="wikidata":
            return_df = self.download_wikidata(supplied_data, generate_metadata)
        return return_df

    def download_general(self, supplied_data: d3m_DataFrame=None, generate_metadata=True) -> d3m_DataFrame:
        candidate_join_column_pairs = self.get_join_hints()
        if len(candidate_join_column_pairs) > 1:
            print("[WARN]: multiple joining column pairs found")
        join_pairs_result = []
        candidate_join_column_scores = []

        # start finding pairs
        if supplied_data is None:
            supplied_data = self.supplied_data
        left_df = supplied_data
        right_metadata = self.search_result['_source']
        right_df = Utils.materialize(metadata=self.metadata)
        left_metadata = Utils.generate_metadata_from_dataframe(data=left_df, original_meta=None)

        # generate the pairs for each join_column_pairs
        for each_pair in candidate_join_column_pairs:
            left_columns = each_pair.left_columns
            right_columns = each_pair.right_columns
            try:
                # Only profile the joining columns, otherwise it will be too slow:
                left_metadata = Utils.calculate_dsbox_features(data=left_df, metadata=left_metadata,
                                                               selected_columns=set(left_columns))

                right_metadata = Utils.calculate_dsbox_features(data=right_df, metadata=right_metadata,
                                                                selected_columns=set(right_columns))
                # update with implicit_variable on the user supplied dataset
                if left_metadata.get('implicit_variables'):
                    Utils.append_columns_for_implicit_variables_and_add_meta(left_metadata, left_df)

                print(" - start getting pairs for", each_pair)

                result, self.pairs = RLTKJoiner.find_pair(left_df=left_df, right_df=right_df, left_columns=[left_columns],
                                             right_columns=[right_columns], left_metadata=left_metadata,
                                             right_metadata=right_metadata)

                join_pairs_result.append(result)
                # TODO: figure out some way to compare the joining quality
                candidate_join_column_scores.append(100)
            except:
                print("failed when getting pairs for", each_pair)
                traceback.print_exc()

        # choose the best joining results
        all_results = []
        for i in range(len(join_pairs_result)):
            each_result = (candidate_join_column_pairs[i], candidate_join_column_scores[i], join_pairs_result[i])
            all_results.append(each_result)

        all_results.sort(key=lambda x: x[1], reverse=True)

        return_df = d3m_DataFrame(all_results[0][2], generate_metadata=False)
        if generate_metadata:
            metadata_selector = (ALL_ELEMENTS,)
            metadata_dimension_columns = {"name": "columns",
                                          "semantic_types": (
                                              "https://metadata.datadrivendiscovery.org/types/TabularColumn",),
                                          "length": return_df.shape[1]}
            # d3m require it to be frozen ordered dict
            metadata_dimension_columns = frozendict.FrozenOrderedDict(metadata_dimension_columns)
            metadata_all_elements = {"dimension": metadata_dimension_columns}
            metadata_all_elements = frozendict.FrozenOrderedDict(metadata_all_elements)
            return_df.metadata = return_df.metadata.update(metadata=metadata_all_elements, selector=metadata_selector)

            # in the case of further more restricted check, also add metadata query of ()
            metadata_selector = ()
            metadata_dimension_rows = {"name": "rows",
                                       "semantic_types": ("https://metadata.datadrivendiscovery.org/types/TabularRow",),
                                       "length": return_df.shape[0]}
            # d3m require it to be frozen ordered dict
            metadata_dimension_rows = frozendict.FrozenOrderedDict(metadata_dimension_rows)
            metadata_all = {"structural_type": d3m_DataFrame,
                            "semantic_types": ("https://metadata.datadrivendiscovery.org/types/Table",),
                            "dimension": metadata_dimension_rows,
                            "schema": "https://metadata.datadrivendiscovery.org/schemas/v0/container.json"}
            metadata_all = frozendict.FrozenOrderedDict(metadata_all)
            return_df.metadata = return_df.metadata.update(metadata=metadata_all, selector=metadata_selector)
            for i, each_metadata in enumerate(self.metadata['variables']):
                metadata_selector = (ALL_ELEMENTS, i)
                structural_type = each_metadata["description"].split("dtype: ")[-1]
                if "int" in structural_type:
                    structural_type = int
                elif "float" in structural_type:
                    structural_type = float
                else:
                    structural_type = str
                metadata_each_column = {"name": each_metadata["name"], "structural_type": structural_type,
                                        'semantic_types': ('https://metadata.datadrivendiscovery.org/types/Attribute',)}
                return_df.metadata = return_df.metadata.update(metadata=metadata_each_column,
                                                               selector=metadata_selector)

            metadata_selector = (ALL_ELEMENTS, return_df.shape[1])
            metadata_joining_pairs = {"name": "joining_pairs", "structural_type": typing.List[int],
                                      'semantic_types': ("http://schema.org/Integer",)}
            return_df.metadata = return_df.metadata.update(metadata=metadata_joining_pairs, selector=metadata_selector)

        return return_df

    def download_wikidata(self, supplied_data: d3m_DataFrame, generate_metadata=True) -> d3m_DataFrame:
        """
        :param supplied_data: input DataFrame
        :param generate_metadata: control whether to automatically generate metadata of the return DataFrame or not
        :return: return_df: the materialized wikidata d3m_DataFrame,
                            with corresponding pairing information to original_data at last column
        """
        # prepare the query
        p_nodes_needed = self.search_result["p_nodes_needed"]
        target_q_node_column_name = self.search_result["target_q_node_column_name"]
        q_node_column_number = supplied_data.columns.tolist().index(target_q_node_column_name)
        q_nodes_list = supplied_data.iloc[:, q_node_column_number].tolist()
        q_nodes_query = ""
        p_nodes_query_part = ""
        p_nodes_optional_part = ""
        for each in q_nodes_list:
            q_nodes_query += "(wd:" + each + ") \n"
        for each in p_nodes_needed:
            p_nodes_query_part += " ?" + each
            p_nodes_optional_part += "  OPTIONAL { ?q wdt:" + each + " ?" + each + "}\n"
        sparql_query = "SELECT ?q " + p_nodes_query_part + \
                       "WHERE \n{\n  VALUES (?q) { \n " + q_nodes_query + "}\n" + \
                       p_nodes_optional_part + "}\n"

        return_df = d3m_DataFrame()
        try:
            sparql = SPARQLWrapper("http://sitaware.isi.edu:8080/bigdata/namespace/wdq/sparql")
            sparql.setQuery(sparql_query)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()
        except:
            print("Getting query of wiki data failed!")
            return return_df

        semantic_types_dict = {
            "q_node": ("http://schema.org/Text", 'https://metadata.datadrivendiscovery.org/types/PrimaryKey')}
        for result in results["results"]["bindings"]:
            each_result = {}
            q_node_name = result.pop("q")["value"].split("/")[-1]
            each_result["q_node"] = q_node_name
            for p_name, p_val in result.items():
                each_result[p_name] = p_val["value"]
                # only do this part if generate_metadata is required
                if generate_metadata and p_name not in semantic_types_dict:
                    if "datatype" in p_val.keys():
                        semantic_types_dict[p_name] = (
                            self._get_semantic_type(p_val["datatype"]),
                            'https://metadata.datadrivendiscovery.org/types/Attribute')
                    else:
                        semantic_types_dict[p_name] = (
                            "http://schema.org/Text", 'https://metadata.datadrivendiscovery.org/types/Attribute')

            return_df = return_df.append(each_result, ignore_index=True)

        p_name_dict = {"q_node": "q_node"}
        for each in return_df.columns.tolist():
            if each[0] == "P":
                p_name_dict[each] = self._get_node_name(each)

        # use rltk joiner to find the joining pairs
        joiner = RLTKJoiner_new()
        joiner.set_join_target_column_name((supplied_data.columns[q_node_column_number], "q_node"))
        result, self.pairs = joiner.find_pair(left_df=supplied_data, right_df=return_df)

        if generate_metadata:
            metadata_selector = (ALL_ELEMENTS,)
            metadata_dimension_columns = {"name": "columns",
                                          "semantic_types": (
                                              "https://metadata.datadrivendiscovery.org/types/TabularColumn",),
                                          "length": return_df.shape[1]}
            # d3m require it to be frozen ordered dict
            metadata_dimension_columns = frozendict.FrozenOrderedDict(metadata_dimension_columns)
            metadata_all_elements = {"dimension": metadata_dimension_columns}
            metadata_all_elements = frozendict.FrozenOrderedDict(metadata_all_elements)
            return_df.metadata = return_df.metadata.update(metadata=metadata_all_elements, selector=metadata_selector)

            # in the case of further more restricted check, also add metadata query of ()
            metadata_selector = ()
            metadata_dimension_rows = {"name": "rows",
                                       "semantic_types": ("https://metadata.datadrivendiscovery.org/types/TabularRow",),
                                       "length": return_df.shape[0]}
            # d3m require it to be frozen ordered dict
            metadata_dimension_rows = frozendict.FrozenOrderedDict(metadata_dimension_rows)
            metadata_all = {"structural_type": d3m_DataFrame,
                            "semantic_types": ("https://metadata.datadrivendiscovery.org/types/Table",),
                            "dimension": metadata_dimension_rows,
                            "schema": "https://metadata.datadrivendiscovery.org/schemas/v0/container.json"}
            metadata_all = frozendict.FrozenOrderedDict(metadata_all)
            return_df.metadata = return_df.metadata.update(metadata=metadata_all, selector=metadata_selector)

            # add remained attributes metadata
            for each_column in range(0, return_df.shape[1] - 1):
                current_column_name = p_name_dict[return_df.columns[each_column]]
                metadata_selector = (ALL_ELEMENTS, each_column)
                # here we do not modify the original data, we just add an extra "expected_semantic_types" to metadata
                metadata_each_column = {"name": current_column_name, "structural_type": str,
                                        'semantic_types': semantic_types_dict[return_df.columns[each_column]]}
                return_df.metadata = return_df.metadata.update(metadata=metadata_each_column,
                                                               selector=metadata_selector)

            # special for joining_pairs column
            metadata_selector = (ALL_ELEMENTS, return_df.shape[1])
            metadata_joining_pairs = {"name": "joining_pairs", "structural_type": typing.List[int],
                                      'semantic_types': ("http://schema.org/Integer",)}
            return_df.metadata = return_df.metadata.update(metadata=metadata_joining_pairs, selector=metadata_selector)

        # update column names to be property names instead of number
        return_df = return_df.rename(columns=p_name_dict)

        return return_df

    def _get_node_name(self, node_code):
        """
        Inner function used to get the properties(P nodes) names with given P node
        :param node_code: a str indicate the P node (e.g. "P123")
        :return: a str indicate the P node label (e.g. "inception")
        """
        sparql_query = "SELECT DISTINCT ?x WHERE \n { \n" + \
                       "wd:" + node_code + " rdfs:label ?x .\n FILTER(LANG(?x) = 'en') \n} "
        try:
            sparql = SPARQLWrapper("http://sitaware.isi.edu:8080/bigdata/namespace/wdq/sparql")
            sparql.setQuery(sparql_query)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()
            return results['results']['bindings'][0]['x']['value']
        except:
            print("Getting name of node " + node_code + " failed!")
            return node_code

    def _get_semantic_type(self, datatype: str):
        """
        Inner function used to transfer the wikidata semantic type to D3M semantic type
        :param datatype: a str indicate the semantic type adapted from wikidata
        :return: a str indicate the semantic type for D3M
        """
        special_type_dict = {"http://www.w3.org/2001/XMLSchema#dateTime": "http://schema.org/DateTime",
                             "http://www.w3.org/2001/XMLSchema#decimal": "http://schema.org/Float",
                             "http://www.opengis.net/ont/geosparql#wktLiteral": "https://metadata.datadrivendiscovery.org/types/Location"}
        default_type = "http://schema.org/Text"
        if datatype in special_type_dict:
            return special_type_dict[datatype]
        else:
            print("not seen type : ", datatype)
            return default_type

    def augment(self, supplied_data, generate_metadata=True):
        """
        download and join using the D3mJoinSpec from get_join_hints()
        """
        if type(supplied_data) is d3m_DataFrame:
            return self._augment(supplied_data=supplied_data, generate_metadata=generate_metadata, type="df")
        elif type(supplied_data) is d3m_Dataset:
            self._res_id, self.supplied_data = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None, has_hyperparameter=False)
            res = self._augment(supplied_data=supplied_data, generate_metadata=generate_metadata, type="ds")
            return res

    def _augment(self, supplied_data, generate_metadata=True, type="df"):
        """
        download and join using the D3mJoinSpec from get_join_hints()
        """
        if type=="ds":
            supplied_data_df = supplied_data[self._res_id]
        elif type=="df":
            supplied_data_df = supplied_data

        download_result = self.download(supplied_data=supplied_data_df, generate_metadata=False)
        download_result = download_result.drop(columns=['joining_pairs'])
        df_joined = pd.DataFrame()

        column_names_to_join = None
        for r1, r2 in self.pairs:
            left_res = supplied_data_df.loc[int(r1)]
            right_res = download_result.loc[int(r2)]
            if column_names_to_join is None:
                column_names_to_join = right_res.index.difference(left_res.index)
                matched_rows = right_res.index.intersection(left_res.index)
                columns_new = left_res.index.tolist()
                columns_new.extend(column_names_to_join.tolist())
            new = pd.concat([left_res, right_res[column_names_to_join]])
            df_joined = df_joined.append(new, ignore_index=True)
        # ensure that the original dataframe columns are at the first left part
        df_joined = df_joined[columns_new]
        df_joined_d3m = d3m_DataFrame(df_joined, generate_metadata=False, dtype=str)
        if generate_metadata:
            columns_all = list(df_joined.columns)
            if 'd3mIndex' in df_joined.columns:
                oldindex = columns_all.index('d3mIndex')
                columns_all.insert(0, columns_all.pop(oldindex))
            else:
                print("No d3mIndex column found after data-mart augment!!!")
            df_joined = df_joined[columns_all]

            # start adding metadata for dataset
            metadata_dict_left = {}
            metadata_dict_right = {}
            for each in self.metadata['variables']:
                decript = each['description']
                dtype = decript.split("dtype: ")[-1]
                if "float" in dtype:
                    semantic_types = (
                        "http://schema.org/Float",
                        "https://metadata.datadrivendiscovery.org/types/Attribute"
                    )
                elif "int" in dtype:
                    semantic_types = (
                        "http://schema.org/Integer",
                        "https://metadata.datadrivendiscovery.org/types/Attribute"
                    )
                else:
                    semantic_types = (
                        "https://metadata.datadrivendiscovery.org/types/CategoricalData",
                        "https://metadata.datadrivendiscovery.org/types/Attribute"
                    )
                each_meta = {
                    "name": each['name'],
                    "structural_type": str,
                    "semantic_types": semantic_types,
                    "description": decript
                }
                metadata_dict_right[each['name']] = frozendict.FrozenOrderedDict(each_meta)
            if type == "df":
                left_df_column_legth = supplied_data.metadata.query((metadata_base.ALL_ELEMENTS,))['dimension'][
                    'length']
            elif type == "ds":
                left_df_column_legth = supplied_data.metadata.query((self._res_id, metadata_base.ALL_ELEMENTS,))['dimension']['length']

            for i in range(left_df_column_legth):
                if type == "df":
                    each_selector = (ALL_ELEMENTS, i)
                elif type == "ds":
                    each_selector = (self._res_id, ALL_ELEMENTS, i)
                each_column_meta = supplied_data.metadata.query(each_selector)
                metadata_dict_left[each_column_meta['name']] = each_column_meta

            metadata_new = metadata_base.DataMetadata()
            metadata_old = copy.copy(supplied_data.metadata)
            # for dataset, we need to update () query first
            if type=="ds":
                metadata_new = metadata_new.update((), metadata_old.query(()))

            # update column metadata
            if type == "df":
                selector = (ALL_ELEMENTS,)
            elif type == "ds":
                selector = (self._res_id, ALL_ELEMENTS,)
            new_column_meta = dict(metadata_old.query(selector))
            new_column_meta['dimension'] = dict(new_column_meta['dimension'])
            new_column_meta['dimension']['length'] = df_joined.shape[1]
            metadata_new = metadata_new.update(selector, new_column_meta)

            # update row metadata
            if type=="df":
                selector = ()
            elif type=="ds":
                selector = (self._res_id,)
            new_row_meta = dict(metadata_old.query(selector))
            new_row_meta['dimension'] = dict(new_row_meta['dimension'])
            new_row_meta['dimension']['length'] = df_joined.shape[0]
            new_row_meta['dimension'] = frozendict.FrozenOrderedDict(new_row_meta['dimension'])
            new_row_meta = frozendict.FrozenOrderedDict(new_row_meta)
            metadata_new = metadata_new.update(selector, new_row_meta)

            new_column_names_list = list(df_joined.columns)
            # update each column's metadata
            for i, current_column_name in enumerate(new_column_names_list):
                if type == "df":
                    each_selector = (metadata_base.ALL_ELEMENTS, i)
                elif type == "ds":
                    each_selector = (self._res_id, ALL_ELEMENTS, i)
                if current_column_name in metadata_dict_left:
                    new_metadata_i = metadata_dict_left[current_column_name]
                else:
                    new_metadata_i = metadata_dict_right[current_column_name]
                metadata_new = metadata_new.update(each_selector, new_metadata_i)
            if type=="df":
                df_joined_d3m.metadata = metadata_new
                return df_joined_d3m
            else:
                augmented_dataset = supplied_data.copy()
                augmented_dataset.metadata = metadata_new
                augmented_dataset[self._res_id] = df_joined_d3m
                return augmented_dataset

    def get_score(self) -> float:
        return self.score

    def get_metadata(self) -> dict:
        return self.metadata

    def get_join_hints(self, supplied_data=None) -> typing.List[JOIN_SPEC]:
        """
        Returns hints for joining supplied data with the data that can be downloaded using this search result.
        In the typical scenario, the hints are based on supplied data that was provided when search was called.

        The optional supplied_data argument enables the caller to request recomputation of join hints for specific data.

        :return: a list of join hints. Note that datamart is encouraged to return join hints but not required to do so.
        """
        if not supplied_data:
            supplied_data = self.supplied_data
        if self.search_type == "general":
            inner_hits = self.search_result.get('inner_hits', {})
            results = []
            used = set()
            left = []
            right = []
            for key_path, outer_hits in inner_hits.items():
                vars_type, index, ent_type = key_path.split('.')
                if vars_type != 'required_variables':
                    continue
                left_index = []
                right_index = []
                index = int(index)
                if ent_type == JSONQueryManager.DATAFRAME_COLUMNS:
                    if self.query_json[vars_type][index].get('index'):
                        left_index = self.query_json[vars_type][index].get('index')
                    elif self.query_json[vars_type][index].get('names'):
                        left_index = [supplied_data.columns.tolist().index(idx)
                                      for idx in self.query_json[vars_type][index].get('names')]

                    inner_hits = outer_hits.get('hits', {})
                    hits_list = inner_hits.get('hits')
                    if hits_list:
                        for hit in hits_list:
                            offset = hit['_nested']['offset']
                            if offset not in used:
                                right_index.append(offset)
                                used.add(offset)
                                break

                if left_index and right_index:
                    each_result = D3MJoinSpec(left_columns=left_index, right_columns=right_index)
                    results.append(each_result)

            return results

    @classmethod
    def construct(cls, serialization: dict) -> SEARCH_RESULT:
        """
        Take into the serilized input and reconsctruct a "DataMartSearchResult"
        """
        load_result = DataMartSearchResult(search_result=serialization["search_result"],
                                           supplied_data=None,
                                           query_json=serialization["query_json"],
                                           search_type=serialization["search_type"])
        return load_result

    def serialize(self) -> dict:
        output = {}
        output["search_result"] = self.search_result
        output["query_json"] = self.query_json
        output["search_type"] = self.search_type
        return output

class D3MJoinSpec:
    """
    A join spec specifies a possible way to join a left datasets with a right dataset. The spec assumes that it may
    be necessary to use several columns in each datasets to produce a key or fingerprint that is useful for joining
    datasets. The spec consists of two lists of column identifiers or names (left_columns, left_column_names and
    right_columns, right_column_names).

    In the simplest case, both left and right are singleton lists, and the expectation is that an appropriate
    matching function exists to adequately join the datasets. In some cases equality may be an appropriate matching
    function, and in some cases fuzz matching is required. The spec join spec does not specify the matching function.

    In more complex cases, one or both left and right lists contain several elements. For example, the left list
    may contain columns for "city", "state" and "country" and the right dataset contains an "address" column. The join
    spec pairs up ["city", "state", "country"] with ["address"], but does not specify how the matching should be done
    e.g., combine the city/state/country columns into a single column, or split the address into several columns.
    """
    def __init__(self, left_columns: typing.List[int], right_columns: typing.List[int]):
        self.left_columns = left_columns
        self.right_columns = right_columns
        # we can have list of the joining column pairs
        # each list inside left_columns/right_columns is a candidate joining column for that dataFrame
        # each candidate joining column can also have multiple columns
