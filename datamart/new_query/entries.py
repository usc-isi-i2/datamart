import pandas as pd
# import os
import copy
import random
import frozendict
import collections
import typing
from d3m.container import DataFrame as d3m_DataFrame
from d3m.container import Dataset as d3m_Dataset
import d3m.metadata.base as metadata_base
# from datamart.dataset import Dataset
from datamart.utilities.utils import PRODUCTION_ES_INDEX, SEARCH_URL
from datamart.es_managers.json_query_manager import JSONQueryManager
from datamart.new_query.augment import Augment
# old Augment 
# from datamart.augment import Augment
# from datamart.data_loader import DataLoader
from d3m.base import utils as d3m_utils
from datamart.utilities.utils import Utils
# from datamart.joiners.join_result import JoinResult
# from datamart.joiners.joiner_base import JoinerType
# from itertools import chain
from datamart.joiners.rltk_joiner import RLTKJoiner
from SPARQLWrapper import SPARQLWrapper, JSON, POST, URLENCODED
from d3m.metadata.base import DataMetadata, ALL_ELEMENTS
from datamart.joiners.rltk_joiner import RLTKJoiner_new
from wikifier import config
# import requests
import traceback
import logging
import datetime
import enum

Q_NODE_SEMANTIC_TYPE = "http://wikidata.org/qnode"
DEFAULT_URL = "https://isi-datamart.edu"
__ALL__ = ('D3MDatamart', 'DatamartSearchResult', 'D3MJoinSpec')
DatamartSearchResult = typing.TypeVar('DatamartSearchResult', bound='DatamartSearchResult')
D3MJoinSpec = typing.TypeVar('D3MJoinSpec', bound='D3MJoinSpec')
DatamartQuery = typing.TypeVar('DatamartQuery', bound='DatamartQuery')
MAX_ENTITIES_LENGTH = 200
CONTAINER_SCHEMA_VERSION = 'https://metadata.datadrivendiscovery.org/schemas/v0/container.json'
P_NODE_IGNORE_LIST = {"P1549"}
SPECIAL_REQUEST_FOR_P_NODE = {"P1813": "FILTER(strlen(str(?P1813)) = 2)"}
AUGMENT_RESOURCE_ID = "augmentData"


class D3MDatamart:
    """
    ISI implementation of D3MDatamart
    """
    def __init__(self, mode):
        self.url = DEFAULT_URL
        self._logger = logging.getLogger(__name__)
        if mode == "test":
            query_server = config.endpoint_query_test
        else:
            query_server = config.endpoint_query_main
        self.augmenter = Augment(endpoint=query_server)

    def search(self, query: DatamartQuery, timeout=None, limit: int = 20) -> typing.List[DatamartSearchResult]:
        """
        This entry point supports search using a query specification.
        The query spec is rich enough to enable query by example. The caller can select
        values from a column in a dataset and use the query spec to find datasets that can join with the values
        provided. Use of the query spec enables callers to compose their own "smart search" implementations.

        Parameters
        ----------
        query: DatamartQuery
            Query specification.
        timeout: int
            Maximum number of seconds before returning results.
        limit: int
            Maximum number of search results to return.

        returns
        -------
        typing.List[DatamartSearchResult]
            List of search results, combined from multiple datamarts. In the baseline implementation the list of
            datamarts will be randomized on each call, and the results from multiple datamarts will be interleaved
            accordingly. There will be an attempt to deduplicate results from multiple datamarts.
        """
        if not self.url.startswith(SEARCH_URL):
            return []
        query_json = query.to_json()
        return self.search_general(query=query_json, supplied_data=None, timeout=timeout, limit=limit)

    def search_general(self, query, supplied_data: d3m_DataFrame=None, timeout=None, limit: int=20) \
            -> typing.List[DatamartSearchResult]:
        """
        The search function used for general elastic search
        :param query: JSON object describing the query.
        :param supplied_data: the data you are trying to augment.
        :param timeout: allowed time spent on searching
        :param limit: the limitation on the return amount of DatamartSearchResult
        :return: list of search results of DatamartSearchResult
        """
        query_results = []
        try:
            params = {"timeout": "2m"}
            if (query and ('required_variables' in query)) or (supplied_data is None):
                # if ("required_variables" exists or no data):

                cur_results = self.augmenter.query_by_sparql(query, supplied_data, size=limit) or []
                for res in cur_results:
                    query_results.append(DatamartSearchResult(search_result=res, supplied_data=supplied_data,
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
                        cur_results = self.augmenter.query_by_sparql(query_copy, supplied_data, size=limit, params=params)
                        if not cur_results:
                            continue
                        for res in cur_results:
                            if res['_id'] not in exist:
                                # TODO: how about the score ??
                                exist.add(res['_id'])
                            query_results.append(DatamartSearchResult(search_result=res, supplied_data=supplied_data,
                                                                   query_json=query_copy, search_type="general")
                                              )
            return query_results
        except:
            print("Searching with wiki data failed")
            traceback.print_exc()
        finally:
            return query_results

    def search_with_data(self, supplied_data: typing.Union[d3m_Dataset, d3m_DataFrame], query: DatamartQuery = None,
                         timeout=None, limit: int = 20) -> typing.List[DatamartSearchResult]:
        """
        This entry point supports search based on a supplied datasets.

        *) Smart search: the caller provides supplied_data (a D3M dataset), and a query containing
        keywords from the D3M problem specification. The search will analyze the supplied data and look for datasets
        that can augment the supplied data in some way. The keywords are used for further filtering and ranking.
        For example, a datamart may try to identify named entities in the supplied data and search for companion
        datasets that contain these entities.

        *) Columns search: this search is similar to smart search in that it uses both query spec and the supplied_data.
        The difference is that only the data in the columns listed in query_by_example_columns is used to identify
        companion datasets.

        Parameters
        ---------
        query: DatamartQuery
            Query specification
        supplied_data: d3m.container.Dataset or d3m.container.DataFrame
            The data you are trying to augment.
        query_by_example_columns:
            A list of column identifiers or column names.
        timeout: int
            Maximum number of seconds before returning results.
        limit: int
            Maximum number of search results to return.


        Returns
        -------
        typing.List[DatamartSearchResult]
            A list of DatamartSearchResult of possible companion datasets for the supplied data.
        """

        if not self.url.startswith(SEARCH_URL):
            return []

        if type(supplied_data) is d3m_Dataset:
            res_id, supplied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None)
        else:
            supplied_dataframe = supplied_data

        search_results = []
        if query is not None:
            query_json = query.to_json()
        else:
            query_json = None

        # first take a search on wikidata
        wikidata_search_results = self.search_wiki_data(query_json, supplied_data, timeout)
        search_results.extend(wikidata_search_results)
        limit_remained = limit - len(wikidata_search_results)

        if query is None:
            # if not query given, try to find the Text columns from given dataframe and use it to find some candidates
            can_query_columns = []
            for each in range(len(supplied_dataframe.columns)):
                if type(supplied_data) is d3m_Dataset:
                    selector = (res_id, ALL_ELEMENTS, each)
                else:
                    selector = (ALL_ELEMENTS, each)
                each_column_meta = supplied_data.metadata.query(selector)
                if 'http://schema.org/Text' in each_column_meta["semantic_types"]:
                    # or "https://metadata.datadrivendiscovery.org/types/CategoricalData" in each_column_meta["semantic_types"]:
                    can_query_columns.append(each)

            # import pdb
            # pdb.set_trace()

            if len(can_query_columns) == 0:
                self._logger.warning("No columns can be augment!")
                return search_results

            results_no_query = []
            for each_column in can_query_columns:
                column_values = supplied_dataframe.iloc[:, each_column]
                query_column_entities = list(set(column_values.tolist()))

                if len(query_column_entities) > MAX_ENTITIES_LENGTH:
                    query_column_entities = random.sample(query_column_entities, MAX_ENTITIES_LENGTH)

                for i in range(len(query_column_entities)):
                    query_column_entities[i] = str(query_column_entities[i])

                query_column_entities = " ".join(query_column_entities)

                search_query = DatamartQuery(about=query_column_entities)
                query_json = search_query.to_json()
                # TODO: need to improve the query for required variables
                # search_query ={
                #     "required_variables": [
                #         {
                #             "type": "generic_entity",
                #             "named_entities":query_column_entities
                #         }
                #     ]
                # }

                # sort to put best results at top
                temp_results = self.search_general(query=query_json, supplied_data=supplied_dataframe)
                temp_results.sort(key=lambda x: x.score, reverse=True)
                results_no_query.append(temp_results)
            # we will return the results of each searching query one by one
            # for example: [res_q1_1,res_q1_2,res_q1_3], [res_q2_1,res_q2_2,res_q2_3] , [res_q3_1,res_q3_2,res_q3_3]
            # will return as: [res_q1_1, res_q2_1, res_q3_1, res_q1_2, res_q2_2, res_q3_3...]
            results_rescheduled = []
            has_remained = True
            while has_remained:
                has_remained = False
                for each in results_no_query:
                    if len(each) > 0:
                        has_remained = True
                        results_rescheduled.append(each.pop(0))
            # append together
            search_results.extend(results_rescheduled)

        else:  # for the condition if given query, follow the query
            if limit_remained > 0:
                query_json = query.to_json()
                general_search_results = self.search_general(query=query_json, supplied_data=supplied_dataframe,
                                                             timeout=timeout, limit=limit_remained)
                general_search_results.sort(key=lambda x: x.score, reverse=True)
                search_results.extend(general_search_results)

        if len(search_results) > limit:
            search_results = search_results[:limit]
        return search_results

    def augment(self, query, supplied_data: d3m_Dataset, timeout: int = None, max_new_columns: int = 1000) -> d3m_Dataset:
        """
        In this entry point, the caller supplies a query and a dataset, and datamart returns an augmented dataset.
        Datamart automatically determines useful data for augmentation and automatically joins the new data to produce
        an augmented Dataset that may contain new columns and rows, and possibly new dataframes.

        Parameters
        ---------
        query: DatamartQuery
            Query specification
        supplied_data: d3m.container.Dataset
            The data you are trying to augment.
        timeout: int
            Maximum number of seconds before returning results.
        max_new_columns: int
            Maximum number of new columns to add to the original dataset.

        Returns
        -------
        d3m.container.Dataset
            The augmented Dataset
        """
        if type(supplied_data) is d3m_Dataset:
            input_type = "ds"
            res_id, _ = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None)
        else:
            input_type = "df"

        search_results = self.search_with_data(query=query, supplied_data=supplied_data, timeout=timeout)
        continue_aug = True
        count = 0
        augment_result = supplied_data

        # continue augmenting until reach the maximum new columns or all search_result has been used
        while continue_aug and count < len(search_results):
            augment_result = search_results[count].augment(supplied_data=augment_result)
            count += 1
            if input_type == "ds":
                current_column_number = augment_result[res_id].shape[1]
            else:
                current_column_number = augment_result.shape[1]
            if current_column_number >= max_new_columns:
                continue_aug = False

        return augment_result

    @staticmethod
    def search_wiki_data(query, supplied_data: typing.Union[d3m_DataFrame, d3m_Dataset]=None, timeout=None,
                         search_threshold=0.5) -> typing.List[DatamartSearchResult]:
        """
        The search function used for wikidata search
        :param query: JSON object describing the query.
        :param supplied_data: the data you are trying to augment.
        :param timeout: allowed time spent on searching
        :param limit: the limitation on the return amount of DatamartSearchResult
        :param search_threshold: the minimum appeared times of the properties
        :return: list of search results of DatamartSearchResult
        """
        wikidata_results = []
        try:
            q_nodes_columns = []
            if type(supplied_data) is d3m_Dataset:
                res_id, supplied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None)
                selector_base_type = "ds"
            else:
                supplied_dataframe = supplied_data
                selector_base_type = "df"

            # check whether Qnode is given in the inputs, if given, use this to wikidata and search
            required_variables_names = None
            metadata_input = supplied_data.metadata

            if query is not None and 'required_variables' in query:
                required_variables_names = []
                for each in query['required_variables']:
                    required_variables_names.extend(each['names'])
            for i in range(supplied_dataframe.shape[1]):
                if selector_base_type == "ds":
                    metadata_selector = (res_id, metadata_base.ALL_ELEMENTS, i)
                else:
                    metadata_selector = (metadata_base.ALL_ELEMENTS, i)
                if Q_NODE_SEMANTIC_TYPE in metadata_input.query(metadata_selector)["semantic_types"]:
                    # if no required variables given, attach any Q nodes found
                    if required_variables_names is None:
                        q_nodes_columns.append(i)
                    # otherwise this column has to be inside required_variables
                    else:
                        if supplied_dataframe.columns[i] in required_variables_names:
                            q_nodes_columns.append(i)

            if len(q_nodes_columns) == 0:
                print("No wikidata Q nodes detected on corresponding required_variables! Will skip wikidata search part")
                return wikidata_results
            else:

                print("Wikidata Q nodes inputs detected! Will search with it.")
                print("Totally " + str(len(q_nodes_columns)) + " Q nodes columns detected!")

                # do a wikidata search for each Q nodes column
                for each_column in q_nodes_columns:
                    q_nodes_list = supplied_dataframe.iloc[:, each_column].tolist()
                    p_count = collections.defaultdict(int)
                    p_nodes_needed = []
                    # temporary block
                    """
                    http_address = 'http://minds03.isi.edu:4444/get_properties'
                    headers = {"Content-Type": "application/json"}
                    requests_data = str(q_nodes_list)
                    requests_data = requests_data.replace("'", '"')
                    r = requests.post(http_address, data=requests_data, headers=headers)
                    results = r.json()
                    for each_p_list in results.values():
                        for each_p in each_p_list:
                            p_count[each_p] += 1
                    """
                    # TODO: temporary change here, may change back in the future
                    # Q node format (wd:Q23)(wd: Q42)
                    q_node_query_part = ""
                    unique_qnodes = set(q_nodes_list)
                    for each in unique_qnodes:
                        if len(each) > 0:
                            q_node_query_part += "(wd:" + each + ")"
                    sparql_query = "select distinct ?item ?property where \n{\n  VALUES (?item) {" + q_node_query_part \
                                   + "  }\n  ?item ?property ?value .\n  ?wd_property wikibase:directClaim ?property ." \
                                   + "  values ( ?type ) \n  {\n    ( wikibase:Quantity )\n" \
                                   + "    ( wikibase:Time )\n    ( wikibase:Monolingualtext )\n  }" \
                                   + "  ?wd_property wikibase:propertyType ?type .\n}\norder by ?item ?property "

                    try:
                        sparql = SPARQLWrapper(WIKIDATA_QUERY_SERVER)
                        sparql.setQuery(sparql_query)
                        sparql.setReturnFormat(JSON)
                        sparql.setMethod(POST)
                        sparql.setRequestMethod(URLENCODED)
                        results = sparql.query().convert()['results']['bindings']
                    except:
                        print("Getting query of wiki data failed!")
                        continue
                    for each in results:
                        p_count[each['property']['value'].split("/")[-1]] += 1

                    for key, val in p_count.items():
                        if float(val) / len(unique_qnodes) >= search_threshold:
                            p_nodes_needed.append(key)
                    wikidata_search_result = {"p_nodes_needed": p_nodes_needed,
                                              "target_q_node_column_name": supplied_dataframe.columns[each_column]}
                    wikidata_results.append(DatamartSearchResult(search_result=wikidata_search_result,
                                                                 supplied_data=supplied_data,
                                                                 query_json=query,
                                                                 search_type="wikidata")
                                            )
            return wikidata_results

        except:
            print("Searching with wiki data failed")
            traceback.print_exc()
        finally:
            return wikidata_results


class DatamartMetadata:
    """
    This class represents the metadata associated with search results.
    """
    def __init__(self, title: str, description: str):
        self.title = title
        self.description = description

    def get_columns(self) -> typing.List[str]:
        """
        :return: a list of strings representing the names of columns in the dataset that can be downloaded from a
        search result.
        """
        pass

    def get_detailed_metadata(self) -> dict:
        """
        Datamart will use a program-wide metadata standard, currently computed by the Harvard team, using contributions
        from other performers. This method returns the standard metadata for a dataset.
        :return: a dict with the standard metadata.
        """
        pass

    def get_datamart_specific_metadata(self) -> dict:
        """
        A datamart implementation may compute additional metadata beyond the program-wide standard metadata.
        :return: a dict with the datamart-specific metadata.
        """
        pass


class DatamartSearchResult:
    """
    This class represents the search results of a datamart search.
    Different datamarts will provide different implementations of this class.

    Attributes
    ----------
    join_hints: typing.List[D3MAugmentSpec]
        Hints for joining supplied data with datamart data

    """
    def __init__(self, search_result, supplied_data, query_json, search_type):
        self._logger = logging.getLogger(__name__)
        self.search_result = search_result
        if "_score" in self.search_result:
            self.score = self.search_result["_score"]
        if "_source" in self.search_result:
            self.metadata = self.search_result["_source"]
        self.supplied_data = supplied_data

        if type(supplied_data) is d3m_Dataset:
            self.res_id, self.supplied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None)
            self.selector_base_type = "ds"
        elif type(supplied_data) is d3m_DataFrame:
            self.supplied_dataframe = supplied_data
            self.selector_base_type = "df"

        self.query_json = query_json
        self.search_type = search_type
        self.pairs = None
        self._res_id = None  # only used for input is Dataset
        self.join_pairs = None

    def display(self) -> pd.DataFrame:
        """
        function used to see what found inside this search result class in a human vision
        :return: a pandas DataFrame
        """
        if self.search_type == "wikidata":
            column_names = []
            for each in self.search_result["p_nodes_needed"]:
                each_name = self._get_node_name(each)
                column_names.append(each_name)
            column_names = ", ".join(column_names)
            required_variable = []
            required_variable.append(self.search_result["target_q_node_column_name"])
            result = pd.DataFrame({"title": "wikidata search result for " \
                                           + self.search_result["target_q_node_column_name"], \
                                   "columns": column_names, "join columns": required_variable}, index=[0])

        elif self.search_type == "general":
            title = self.search_result['_source']['title']
            column_names = []
            required_variable = []
            for each in self.query_json['required_variables']:
                required_variable.append(each['names'])

            for each in self.search_result['_source']['variables']:
                each_name = each['name']
                column_names.append(each_name)
            column_names = ", ".join(column_names)
            result = pd.DataFrame({"title": title, "columns": column_names, "join columns": required_variable}, index=[0])

        return result

    def download(self, supplied_data: typing.Union[d3m_Dataset, d3m_DataFrame], generate_metadata=True, return_format="ds") \
            -> typing.Union[d3m_Dataset, d3m_DataFrame]:
        """
        download the dataset or dataFrame (depending on the input type) and corresponding metadata information of
        search result everytime call download, the DataFrame will have the exact same columns in same order
        """
        if self.search_type=="general":
            return_df = self.download_general(supplied_data, generate_metadata, return_format)
        elif self.search_type=="wikidata":
            return_df = self.download_wikidata(supplied_data, generate_metadata, return_format)

        return return_df

    def download_general(self, supplied_data: typing.Union[d3m_Dataset, d3m_DataFrame]=None, generate_metadata=True,
                         return_format="ds", augment_resource_id = AUGMENT_RESOURCE_ID) -> typing.Union[d3m_Dataset, d3m_DataFrame]:
        """
        Specified download function for general datamart Datasets
        :param supplied_data: given supplied data
        :param generate_metadata: whether need to genreate the metadata or not
        :return: a dataset or a dataframe depending on the input
        """
        if type(supplied_data) is d3m_Dataset:
            self._res_id, self.supplied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None)
        else:
            self.supplied_dataframe = supplied_data

        if self.join_pairs is None:
            candidate_join_column_pairs = self.get_join_hints()
        else:
            candidate_join_column_pairs = self.join_pairs

        if len(candidate_join_column_pairs) > 1:
            print("[WARN]: multiple joining column pairs found")
        join_pairs_result = []
        candidate_join_column_scores = []

        # start finding pairs
        if supplied_data is None:
            supplied_data = self.supplied_dataframe
        left_df = copy.deepcopy(self.supplied_dataframe)
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

                print(" - start getting pairs for", each_pair.to_str_format())

                result, self.pairs = RLTKJoiner.find_pair(left_df=left_df, right_df=right_df,
                                                          left_columns=[left_columns], right_columns=[right_columns],
                                                          left_metadata=left_metadata, right_metadata=right_metadata)

                join_pairs_result.append(result)
                # TODO: figure out some way to compute the joining quality
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
        if len(all_results) == 0:
            raise ValueError("[ERROR] Failed to get pairs!")

        if return_format == "ds":
            return_df = d3m_DataFrame(all_results[0][2], generate_metadata=False)
            resources = {augment_resource_id: return_df}
            return_result = d3m_Dataset(resources=resources, generate_metadata=False)
            if generate_metadata:
                metadata_shape_part_dict = self._generate_metadata_shape_part(value=return_result, selector=())
                for each_selector, each_metadata in metadata_shape_part_dict.items():
                    return_result.metadata = return_result.metadata.update(selector=each_selector, metadata=each_metadata)
                return_result.metadata = self._generate_metadata_column_part_for_general(return_result.metadata, return_format, augment_resource_id)

        elif return_format == "df":
            return_result = d3m_DataFrame(all_results[0][2], generate_metadata=False)
            if generate_metadata:
                metadata_shape_part_dict = self._generate_metadata_shape_part(value=return_result, selector=())
                for each_selector, each_metadata in metadata_shape_part_dict.items():
                    return_result.metadata = return_result.metadata.update(selector=each_selector, metadata=each_metadata)
                return_result.metadata = self._generate_metadata_column_part_for_general(return_result.metadata, return_format, augment_resource_id=None)

        else:
            raise ValueError("Invalid return format was given")

        return return_result

    def _generate_metadata_shape_part(self, value, selector) -> dict:
        """
        recursively generate all metadata for shape part, return a dict
        :param value:
        :param selector:
        :return:
        """
        generated_metadata: dict = {}
        generated_metadata['schema'] = CONTAINER_SCHEMA_VERSION
        if isinstance(value, d3m_Dataset):  # type: ignore
            generated_metadata['dimension'] = {
                'name': 'resources',
                'semantic_types': ['https://metadata.datadrivendiscovery.org/types/DatasetResource'],
                'length': len(value),
            }

            metadata_dict = collections.OrderedDict([(selector, generated_metadata)])

            for k, v in value.items():
                metadata_dict.update(self._generate_metadata_shape_part(v, selector + (k,)))

            # It is unlikely that metadata is equal across dataset resources, so we do not try to compact metadata here.

            return metadata_dict

        if isinstance(value, d3m_DataFrame):  # type: ignore
            generated_metadata['semantic_types'] = ['https://metadata.datadrivendiscovery.org/types/Table']

            generated_metadata['dimension'] = {
                'name': 'rows',
                'semantic_types': ['https://metadata.datadrivendiscovery.org/types/TabularRow'],
                'length': value.shape[0],
            }

            metadata_dict = collections.OrderedDict([(selector, generated_metadata)])

            # Reusing the variable for next dimension.
            generated_metadata = {
                'dimension': {
                    'name': 'columns',
                    'semantic_types': ['https://metadata.datadrivendiscovery.org/types/TabularColumn'],
                    'length': value.shape[1],
                },
            }

            selector_all_rows = selector + (ALL_ELEMENTS,)
            metadata_dict[selector_all_rows] = generated_metadata
            return metadata_dict

    def _generate_metadata_column_part_for_general(self, metadata_return, return_format, augment_resource_id) -> DataMetadata:
        """
        Inner function used to generate metadata for general search
        """
        # part for adding the whole dataset/ dataframe's metadata


        # part for adding each column's metadata
        for i, each_metadata in enumerate(self.metadata['variables']):
            if return_format == "ds":
                metadata_selector = (augment_resource_id, ALL_ELEMENTS, i)
            elif return_format == "df":
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
            metadata_return = metadata_return.update(metadata=metadata_each_column, selector=metadata_selector)

        if return_format == "ds":
            metadata_selector = (augment_resource_id, ALL_ELEMENTS, i + 1)
        elif return_format == "df":
            metadata_selector = (ALL_ELEMENTS, i + 1)
        metadata_joining_pairs = {"name": "joining_pairs", "structural_type": typing.List[int],
                                  'semantic_types': ("http://schema.org/Integer",)}
        metadata_return = metadata_return.update(metadata=metadata_joining_pairs, selector=metadata_selector)

        return metadata_return

    def download_wikidata(self, supplied_data: typing.Union[d3m_Dataset, d3m_DataFrame], generate_metadata=True, return_format="ds",augment_resource_id=AUGMENT_RESOURCE_ID) -> typing.Union[d3m_Dataset, d3m_DataFrame]:
        """
        :param supplied_data: input DataFrame
        :param generate_metadata: control whether to automatically generate metadata of the return DataFrame or not
        :return: return_df: the materialized wikidata d3m_DataFrame,
                            with corresponding pairing information to original_data at last column
        """
        # prepare the query
        p_nodes_needed = self.search_result["p_nodes_needed"]
        target_q_node_column_name = self.search_result["target_q_node_column_name"]
        if type(supplied_data) is d3m_DataFrame:
            self.supplied_dataframe = copy.deepcopy(supplied_data)
        elif type(supplied_data) is d3m_Dataset:
            self._res_id, supplied_dataframe = d3m_utils.get_tabular_resource(dataset=supplied_data,
                                                                                  resource_id=None)
            self.supplied_dataframe = copy.deepcopy(supplied_dataframe)

        q_node_column_number = self.supplied_dataframe.columns.tolist().index(target_q_node_column_name)
        q_nodes_list = set(self.supplied_dataframe.iloc[:, q_node_column_number].tolist())
        q_nodes_query = ""
        p_nodes_query_part = ""
        p_nodes_optional_part = ""
        special_request_part = ""
        # q_nodes_list = q_nodes_list[:30]
        for each in q_nodes_list:
            if each != "N/A":
                q_nodes_query += "(wd:" + each + ") \n"
        for each in p_nodes_needed:
            if each not in P_NODE_IGNORE_LIST:
                p_nodes_query_part += " ?" + each
                p_nodes_optional_part += "  OPTIONAL { ?q wdt:" + each + " ?" + each + "}\n"
            if each in SPECIAL_REQUEST_FOR_P_NODE:
                special_request_part += SPECIAL_REQUEST_FOR_P_NODE[each] + "\n"

        sparql_query = "SELECT DISTINCT ?q " + p_nodes_query_part + \
                       "WHERE \n{\n  VALUES (?q) { \n " + q_nodes_query + "}\n" + \
                       p_nodes_optional_part + special_request_part + "}\n"
        import pdb
        pdb.set_trace()
        return_df = d3m_DataFrame()
        try:
            sparql = SPARQLWrapper(WIKIDATA_QUERY_SERVER)
            sparql.setQuery(sparql_query)
            sparql.setReturnFormat(JSON)
            sparql.setMethod(POST)

            sparql.setRequestMethod(URLENCODED)
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
                if p_name not in semantic_types_dict:
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
            if each.lower().startswith("p") or each.lower().startswith("c"):
                p_name_dict[each] = self._get_node_name(each)

        # use rltk joiner to find the joining pairs
        joiner = RLTKJoiner_new()
        joiner.set_join_target_column_names((self.supplied_dataframe.columns[q_node_column_number], "q_node"))
        result, self.pairs = joiner.find_pair(left_df=self.supplied_dataframe, right_df=return_df)

        # if this condition is true, it means "id" column was added which should not be here
        if return_df.shape[1] == len(p_name_dict) + 2 and "id" in return_df.columns:
            return_df = return_df.drop(columns=["id"])

        metadata_new = DataMetadata()
        self.metadata = {}

        # add remained attributes metadata
        for each_column in range(0, return_df.shape[1] - 1):
            current_column_name = p_name_dict[return_df.columns[each_column]]
            metadata_selector = (ALL_ELEMENTS, each_column)
            # here we do not modify the original data, we just add an extra "expected_semantic_types" to metadata
            metadata_each_column = {"name": current_column_name, "structural_type": str,
                                    'semantic_types': semantic_types_dict[return_df.columns[each_column]]}
            self.metadata[current_column_name] = metadata_each_column
            if generate_metadata:
                metadata_new = metadata_new.update(metadata=metadata_each_column, selector=metadata_selector)

        # special for joining_pairs column
        metadata_selector = (ALL_ELEMENTS, return_df.shape[1])
        metadata_joining_pairs = {"name": "joining_pairs", "structural_type": typing.List[int],
                                  'semantic_types': ("http://schema.org/Integer",)}
        if generate_metadata:
            metadata_new = metadata_new.update(metadata=metadata_joining_pairs, selector=metadata_selector)

        # start adding shape metadata for dataset
        if return_format == "ds":
            return_df = d3m_DataFrame(return_df, generate_metadata=False)
            return_df = return_df.rename(columns=p_name_dict)
            resources = {augment_resource_id: return_df}
            return_result = d3m_Dataset(resources=resources, generate_metadata=False)
            if generate_metadata:
                return_result.metadata = metadata_new
                metadata_shape_part_dict = self._generate_metadata_shape_part(value=return_result, selector=())
                for each_selector, each_metadata in metadata_shape_part_dict.items():
                    return_result.metadata = return_result.metadata.update(selector=each_selector,
                                                                           metadata=each_metadata)
            # update column names to be property names instead of number

        elif return_format == "df":
            return_result = d3m_DataFrame(return_df, generate_metadata=False)
            return_result = return_result.rename(columns=p_name_dict)
            if generate_metadata:
                return_result.metadata = metadata_new
                metadata_shape_part_dict = self._generate_metadata_shape_part(value=return_result, selector=())
                for each_selector, each_metadata in metadata_shape_part_dict.items():
                    return_result.metadata = return_result.metadata.update(selector=each_selector,
                                                                           metadata=each_metadata)
        return return_result

    def _get_node_name(self, node_code):
        """
        Inner function used to get the properties(P nodes) names with given P node
        :param node_code: a str indicate the P node (e.g. "P123")
        :return: a str indicate the P node label (e.g. "inception")
        """
        sparql_query = "SELECT DISTINCT ?x WHERE \n { \n" + \
                       "wd:" + node_code + " rdfs:label ?x .\n FILTER(LANG(?x) = 'en') \n} "
        try:
            sparql = SPARQLWrapper(WIKIDATA_QUERY_SERVER)
            sparql.setQuery(sparql_query)
            sparql.setReturnFormat(JSON)
            sparql.setMethod(POST)
            sparql.setRequestMethod(URLENCODED)
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

    def augment(self, supplied_data, generate_metadata=True, augment_resource_id=AUGMENT_RESOURCE_ID):
        """
        download and join using the D3mJoinSpec from get_join_hints()
        """
        if type(supplied_data) is d3m_DataFrame:
            return self._augment(supplied_data=supplied_data, generate_metadata=generate_metadata, return_format="df", augment_resource_id=augment_resource_id)
        elif type(supplied_data) is d3m_Dataset:
            self._res_id, self.supplied_data = d3m_utils.get_tabular_resource(dataset=supplied_data, resource_id=None, has_hyperparameter=False)
            res = self._augment(supplied_data=supplied_data, generate_metadata=generate_metadata, return_format="ds", augment_resource_id=augment_resource_id)
            return res

    def _augment(self, supplied_data, generate_metadata=True, return_format="ds", augment_resource_id=AUGMENT_RESOURCE_ID):
        """
        download and join using the D3mJoinSpec from get_join_hints()
        """
        if type(supplied_data) is d3m_Dataset:
            supplied_data_df = supplied_data[self._res_id]
        else:
            supplied_data_df = supplied_data

        download_result = self.download(supplied_data=supplied_data_df, generate_metadata=False, return_format="df")
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
        # if search with wikidata, we can remove duplicate Q node column

        if self.search_type == "wikidata":
            df_joined = df_joined.drop(columns=['q_node'])

        if 'id' in df_joined.columns:
            df_joined = df_joined.drop(columns=['id'])

        if generate_metadata:
            columns_all = list(df_joined.columns)
            if 'd3mIndex' in df_joined.columns:
                oldindex = columns_all.index('d3mIndex')
                columns_all.insert(0, columns_all.pop(oldindex))
            else:
                self._logger.warning("No d3mIndex column found after data-mart augment!!!")
            df_joined = df_joined[columns_all]

        # start adding column metadata for dataset
        if generate_metadata:
            metadata_dict_left = {}
            metadata_dict_right = {}
            if self.search_type == "general":
                for each in self.metadata['variables']:
                    description = each['description']
                    dtype = description.split("dtype: ")[-1]
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
                        "description": description
                    }
                    metadata_dict_right[each['name']] = frozendict.FrozenOrderedDict(each_meta)
            else:
                metadata_dict_right = self.metadata

            if return_format == "df":
                left_df_column_legth = supplied_data.metadata.query((metadata_base.ALL_ELEMENTS,))['dimension'][
                    'length']
            elif return_format == "ds":
                left_df_column_legth = supplied_data.metadata.query((self._res_id, metadata_base.ALL_ELEMENTS,))['dimension']['length']

            # add the original metadata
            for i in range(left_df_column_legth):
                if return_format == "df":
                    each_selector = (ALL_ELEMENTS, i)
                elif return_format == "ds":
                    each_selector = (self._res_id, ALL_ELEMENTS, i)
                each_column_meta = supplied_data.metadata.query(each_selector)
                metadata_dict_left[each_column_meta['name']] = each_column_meta

            metadata_new = metadata_base.DataMetadata()
            metadata_old = copy.copy(supplied_data.metadata)

            new_column_names_list = list(df_joined.columns)
            # update each column's metadata
            for i, current_column_name in enumerate(new_column_names_list):
                if return_format == "df":
                    each_selector = (metadata_base.ALL_ELEMENTS, i)
                elif return_format == "ds":
                    each_selector = (augment_resource_id, ALL_ELEMENTS, i)
                if current_column_name in metadata_dict_left:
                    new_metadata_i = metadata_dict_left[current_column_name]
                else:
                    new_metadata_i = metadata_dict_right[current_column_name]
                metadata_new = metadata_new.update(each_selector, new_metadata_i)

            # start adding shape metadata for dataset
            if return_format == "ds":
                return_df = d3m_DataFrame(df_joined, generate_metadata=False)
                resources = {augment_resource_id: return_df}
                return_result = d3m_Dataset(resources=resources, generate_metadata=False)
                if generate_metadata:
                    return_result.metadata = metadata_new
                    metadata_shape_part_dict = self._generate_metadata_shape_part(value=return_result, selector=())
                    for each_selector, each_metadata in metadata_shape_part_dict.items():
                        return_result.metadata = return_result.metadata.update(selector=each_selector,
                                                                               metadata=each_metadata)
            elif return_format == "df":
                return_result = d3m_DataFrame(df_joined, generate_metadata=False)
                if generate_metadata:
                    return_result.metadata = metadata_new
                    metadata_shape_part_dict = self._generate_metadata_shape_part(value=return_result, selector=())
                    for each_selector, each_metadata in metadata_shape_part_dict.items():
                        return_result.metadata = return_result.metadata.update(selector=each_selector,
                                                                               metadata=each_metadata)

            return return_result

    def get_score(self) -> float:
        return self.score

    def get_metadata(self) -> dict:
        return self.metadata

    def set_join_pairs(self, join_pairs: typing.List[D3MJoinSpec]) -> None:
        """
        manually set up the join pairs
        :param join_pairs: user specified D3MJoinSpec
        :return:
        """
        self.join_pairs = join_pairs

    def get_join_hints(self, supplied_data=None) -> typing.List[D3MJoinSpec]:
        """
        Returns hints for joining supplied data with the data that can be downloaded using this search result.
        In the typical scenario, the hints are based on supplied data that was provided when search was called.

        The optional supplied_data argument enables the caller to request recomputation of join hints for specific data.

        :return: a list of join hints. Note that datamart is encouraged to return join hints but not required to do so.
        """
        if not supplied_data:
            supplied_data = self.supplied_dataframe
        if self.search_type == "general":
            inner_hits = self.search_result.get('inner_hits', {})
            results = []
            used = set()
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
    def construct(cls, serialization: dict) -> DatamartSearchResult:
        """
        Take into the serilized input and reconsctruct a "DatamartSearchResult"
        """
        load_result = DatamartSearchResult(search_result=serialization["search_result"],
                                           supplied_data=None,
                                           query_json=serialization["query_json"],
                                           search_type=serialization["search_type"])
        return load_result

    def serialize(self) -> dict:
        output = dict()
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
    def __init__(self, left_columns: typing.List[typing.List[int]], right_columns: typing.List[typing.List[int]],
                 left_resource_id: str=None, right_resource_id: str=None):
        self.left_resource_id = left_resource_id
        self.right_resource_id = right_resource_id
        self.left_columns = left_columns
        self.right_columns = right_columns
        # we can have list of the joining column pairs
        # each list inside left_columns/right_columns is a candidate joining column for that dataFrame
        # each candidate joining column can also have multiple columns

    def to_str_format(self):
        return "[ (" + (self.left_resource_id or "") + ", " + str(self.left_columns) + ") , (" + \
               (self.right_resource_id or "") + ", " + str(self.right_columns) + ") ]"


class TemporalGranularity(enum.Enum):
    YEAR = 1
    MONTH = 2
    DAY = 3
    HOUR = 4
    SECOND = 5


class GeospatialGranularity(enum.Enum):
    COUNTRY = 1
    STATE = 2
    COUNTY = 3
    CITY = 4
    POSTALCODE = 5


class DatamartVariable:
    pass


class DatamartQuery:
    def __init__(self, about: str=None, required_variables: typing.List[DatamartVariable]=None,
                 desired_variables: typing.List[DatamartVariable]=None):
        self.about = about
        self.required_variables = required_variables
        self.desired_variables = desired_variables

    def to_json(self):
        """
        function used to transform the Query to json format that can used on elastic search
        :return:
        """
        search_query = dict()
        if self.about is not None:
            search_query["dataset"] = {
                    "about": self.about
                }
        if self.required_variables is not None:
            search_query["required_variables"] = self.required_variables

        return search_query

class NamedEntityVariable(DatamartVariable):
    """
    Describes columns containing named enitities.

    Parameters
    ----------
    entities: List[str]
        List of strings that should be contained in the matched dataset column.
    """
    def __init__(self, entities: typing.List[str]):
        self.entities = entities


class TemporalVariable(DatamartVariable):
    """
    Describes columns containing temporal information.

    Parameters
    ----------
    start: datetime
        Requested datetime should be equal or older than this datetime
    end: datetime
        Requested datatime should be equal or less than this datetime
    granularity: TemporalGranularity
        Requested datetimes are well matched with the requested granularity
    """
    def __init__(self, start: datetime.datetime, end: datetime.datetime, granularity: TemporalGranularity):
        pass


class GeospatialVariable(DatamartVariable):
    """
    Describes columns containing geospatial information.

    Parameters
    ----------
    lat1: float
        The latitude of the first point
    long1: float
        The longitude of the first point
    lat2: float
        The latitude of the second point
    long2: float
        The longitude of the second point
    granularity: GeospatialGranularity
        Requested geosptial values are well matched with the requested granularity
    """
    def __init__(self, lat1: float, long1: float, lat2: float, long2: float, granularity: GeospatialGranularity):
        pass
