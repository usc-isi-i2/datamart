import pandas as pd
# import os
import frozendict
import collections
import typing
from d3m.container import DataFrame as d3m_DataFrame
import d3m.metadata.base as metadata_base
from datamart.dataset import Dataset
from datamart.utilities.utils import PRODUCTION_ES_INDEX, SEARCH_URL
from datamart.augment import Augment
from datamart.data_loader import DataLoader
import d3m.container.dataset as d3m_ds
from datamart.utilities.utils import Utils
from datamart.joiners.join_result import JoinResult
from datamart.joiners.joiner_base import JoinerType
from itertools import chain
from datamart.joiners.rltk_joiner import RLTKJoiner
from SPARQLWrapper import SPARQLWrapper, JSON
from d3m.metadata.base import ALL_ELEMENTS
from datamart.joiners.rltk_joiner import RLTKJoiner_new
import requests
# import traceback


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

    def search(self, query, supplied_data: d3m_DataFrame=None, timeout=None, limit: int=20) \
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
            for i in range(supplied_data.shape[1]):
                metadata_selector = (metadata_base.ALL_ELEMENTS, i)
                if Q_NODE_SEMANTIC_TYPE in metadata_input.query(metadata_selector)["semantic_types"]:
                    q_nodes_columns.append(i)

            if len(q_nodes_columns) == 0:
                print("No wikidata Q nodes inputs detected! Will skip wikidata search part")
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
                es_results = augmenter.query_by_json(query, supplied_data, size=limit) or []

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
        self.supplied_data = supplied_data
        self.query_json = query_json
        self.search_type = search_type

    def download(self, supplied_data: d3m_DataFrame) -> d3m_DataFrame:
        """
        download the dataFrame and corresponding metadata information of search result
        everytime call download, the DataFrame will have the exact same columns in same order
        """
        if self.search_type=="general":
            return_df = self.download_general(supplied_data)
        elif self.search_type=="wikidata":
            return_df = self.download_wikidata(supplied_data)

        return return_df

    def download_general(self, supplied_data: d3m_DataFrame) -> d3m_DataFrame:
        candidate_join_column_pairs = []
        join_pairs_result = []
        candidate_join_column_scores = []

        # start finding pairs
        left_df = supplied_data
        right_metadata = self.search_result["_metadata"]
        right_df = self.materialize()
        left_metadata = Utils.generate_metadata_from_dataframe(data=left_df, original_meta=None)

        # generate the pairs for each join_column_pairs
        for each_pair in candidate_join_column_pairs:
            left_columns = each_pair[0]
            right_columns = each_pair[1]
            try:
                # Only profile the joining columns, otherwise it will be too slow:
                left_metadata = Utils.calculate_dsbox_features(data=left_df, metadata=left_metadata,
                                                               selected_columns=set(chain.from_iterable(left_columns)))

                right_metadata = Utils.calculate_dsbox_features(data=right_df, metadata=right_metadata,
                                                                selected_columns=set(
                                                                    chain.from_iterable(right_columns)))
                # update with implicit_variable on the user supplied dataset
                if left_metadata.get('implicit_variables'):
                    Utils.append_columns_for_implicit_variables_and_add_meta(left_metadata, left_df)

                print(" - start getting pairs for", each_pair)

                pairs = RLTKJoiner.get_pairs(left_df=left_df, right_df=right_df, left_columns=left_columns,
                                             right_columns=right_columns, left_metadata=left_metadata,
                                             right_metadata=right_metadata)

                join_pairs_result.append(pairs)
            except:
                print("failed when getting pairs for", each_pair)

        # reshape the output format
        join_pairs_return = []
        for i in range(len(join_pairs_result)):
            each_result = (candidate_join_column_pairs[i], candidate_join_column_scores[i], join_pairs_result[i])
            join_pairs_return.append(each_result)

        return_df = d3m_DataFrame(right_df, generate_metadata=generate_metadata)

        return return_df, join_pairs_return

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
        return_df = joiner.find_pair(left_df=supplied_data, right_df=return_df)

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

    def augment(self, supplied_data):
        """
        download and join using the D3mJoinSpec from get_join_hints()
        """
        pass

    def get_score(self) -> float:
        pass

    def get_metadata(self) -> dict:
        pass

    def get_join_hints(self, supplied_data=None) -> typing.List[JOIN_SPEC]:
        """
        If supplied_data not given, will try to find the corresponding joing spec in supplied_data given from search
        """
        pass

    @classmethod
    def construct(cls, serialization:str) -> SEARCH_RESULT:
        """
        Take into the serilized input and reconsctruct a "DataMartSearchResult"
        """
        pass

    def serialize(self) -> str:
        pass

class D3MJoinSpec:
    def __init__(self, left_columns:typing.List[typing.List[int]], right_columns:typing.List[typing.List[int]]):
        self.left_columns = left_columns
        self.right_columns = right_columns
        # we can have list of the joining column pairs
        # each list inside left_columns/right_columns is a candidate joining column for that dataFrame
        # each candidate joining column can also have multiple columns
        """
        For example:
        left_columns = [[1,2],[3]]
        right_columns = [[1],[2]]
        In this example, we have 2 join pairs
        column 1 and 2 in left dataFrame can join to the column 1 in right dataFrame
        column 3 in left dataFrame can join to the column 2 in right dataFrame
        left_dataFrame_column_names = ["id","city","state","country"]
        right_dataFrame_column_names = ["d3mIndex","city-state","country"]
        """