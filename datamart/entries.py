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

def search_old(url: str,
           query: dict,
           data: pd.DataFrame or str or d3m_ds.Dataset=None,
           send_data=True,
           num_results: int=20,
           timeout = None,
           return_named_entity: bool=False) -> typing.List[Dataset]:
    """
    Follow the API defined by https://datadrivendiscovery.org/wiki/display/work/Python+API

    Args:
        url: str - the datamart server(for ISI's datamart it is meaningless, just a flag)
        query: JSON object describing the query(https://datadrivendiscovery.org/wiki/display/work/Query+results+schema)
        data: the data you are trying to augment. It can be provided as one of:
            - a pandas.DataFrame object
            - a D3M Dataset object
            - the path to a D3M datasetDoc.json file
            - the path to a CSV file
        send_data: (for ISI's datamart it is meaningless)

    Returns: a list of datamart.Dataset objects

    """
    if not url.startswith(SEARCH_URL):
        return []

    loaded_data = DataLoader.load_data(data)
    augmenter = Augment(es_index=PRODUCTION_ES_INDEX)

    es_results = []
    if (query and ('required_variables' in query)) or (loaded_data is None):
        # if ("required_variables" exists or no data):
        es_results = augmenter.query_by_json(query, loaded_data,
                                             size=num_results,
                                             return_named_entity=return_named_entity) or []
    else:
        # if there is no "required_variables" in the query JSON, but the dataset exists,
        # try each named entity column as "required_variables" and concat the results:
        query = query or {}
        exist = set()
        for col in loaded_data:
            if Utils.is_column_able_to_query(loaded_data[col]):
                # update 2019.4.9: we should not replace the original query!!!
                query_copy = query.copy()
                query_copy['required_variables'] = [{
                    "type": "dataframe_columns",
                    "names": [col]
                }]
                cur_results = augmenter.query_by_json(query_copy, loaded_data,
                                                   size=num_results,
                                                   return_named_entity=return_named_entity)
                if not cur_results:
                    continue
                for res in cur_results:
                    if res['_id'] not in exist:
                        # TODO: how about the score ??
                        exist.add(res['_id'])
                        es_results.append(res)
    return [Dataset(es_result, original_data=loaded_data, query_json=query) for es_result in es_results]


def augment(original_data: typing.Union[pd.DataFrame, str, d3m_ds.Dataset],
            augment_data: typing.Union[Dataset, dict],
            joining_columns: typing.Union[typing.Tuple[typing.List[typing.List[typing.Union[str, int]]],
                                            typing.List[typing.List[typing.Union[str, int]]]]
                                          , None]=None,
            joiner=JoinerType.RLTK
            ) -> typing.Union[JoinResult, typing.Tuple[JoinResult, str]]:
    """
    Perform the augmentation (either join or union).
    Follow the API defined by https://datadrivendiscovery.org/wiki/display/work/Python+API

    Args:
        original_data: the input data
        augment_data: the found candidate dataset from datamart or the query config (with specified datamart id)
        joining_columns: user defined which columns to be joined(can be either the column names or column numbers)
                         if not given, will try to detect automatically
        destination: the location in disk where the new data will be saved
        format: parameter to control the output format. if format='pandas', where the first item corresponds to the new,
                augmented data, and the second item corresponds to the datasetDoc of the new data; or a d3m.container.Dataset
                object if format='d3m'.
        send_data: if either the entire data, or only the path, will be sent to DataMart;
                   for the latter, DataMart must have access to the path;
                   this option is ignored if data is a pandas.DataFrame object
                   This parameter is useless for isi datamart here
        joiner: the joiner used for joining, currently we have 2 joiner: rltk joiner and pandas joiner
                rltk joiner can do advanced joining like using blocking join (faster) and similar join
                pandas join will only do exact join
    Returns:
        augmented_data: augmented dataFrame


    update 2019.4.8:
    for new structure, we should enable datamart to run augment without search.
    so here now we can pass query with specific id here
    Example of the query for augment_data here
    By giving the dataset id directly here, we can get the target dataset only
    {
        "dataset": {
            "about": "290790000"
        }
    }

    """

    loaded_data = DataLoader.load_data(original_data)
    augmenter = Augment(es_index=PRODUCTION_ES_INDEX)

    # load the query to Dataset if needed
    if type(augment_data) is dict:
        augment_data_query = augment_data
        try:
            es_result = augmenter.query_by_json(augment_data_query, loaded_data, size=1, return_named_entity=False)
            augment_data = Dataset(es_result[0], original_data=loaded_data, query_json=augment_data_query)
        except Exception as e:
            print("Error when loading the given query", e)
            # return the original dataFrame if query failed
            return JoinResult(loaded_data, [])

    '''
    # 2019.3.25 temoprary hack here
    if joining_columns and joiner == JoinerType.RLTK:
        from datamart.joiners.rltk_joiner import RLTKJoiner_new
        try:
            joiner = RLTKJoiner_new()
            joiner.set_join_target_column_name(joining_columns)
            augmented_data = joiner.join(left_df = original_data, right_df = augment_data.materialize())
            return augmented_data
        except:
            traceback.print_exc()
            print("Augment failed!")
            return original_data
    # end temporary hack
    '''
    # if target joining columns was given, set up that columns
    if joining_columns:
        try:
            augment_data.set_join_columns(joining_columns)
        except Exception as e:
            print("FAILED SET JOINING COLUMNS:", e)

    # if no columns can be used for augment, return input
    if not augment_data.join_columns:
        print("No join-able columns found")
        return JoinResult(loaded_data, [])

    # in other condition, try to automatically generate the joining columns
    left_cols, right_cols = augment_data.join_columns

    # !!! if input is d3m, we should already have metadata, why we don't use dataset's original metadata???
    #
    # if type(loaded_data) is d3m_DataFrame:
    #     left_metadata = loaded_data.metadata
    # else:
    #     left_metadata = None

    augmented_data = augmenter.join(
            left_df=loaded_data,
            right_df=augment_data.materialize(),
            left_columns=left_cols,
            right_columns=right_cols,
            left_metadata=None,
            right_metadata=augment_data.metadata,
            joiner=joiner
    )

    return augmented_data

def search(url: str,
           data: pd.DataFrame or str or d3m_ds.Dataset=None,
           num_results: int=20,
           search_threshold: float=0.8,
           ):
    """
    Args:
        url: str - the datamart server(for ISI's datamart it is meaningless, just a flag)
        data: the data you are trying to augment. It can be provided as one of:
            - a pandas.DataFrame object
            - a D3M Dataset object
            - the path to a D3M datasetDoc.json file
            - the path to a CSV file
        num_results: the maximum number of the result candidates want to get returned
        search_threshold: the threshold of the p_nodes returned if it has Q nodes in the inputs, only used for wikidata
    Returns: a list of datamart.Dataset objects

    """
    if not url.startswith(SEARCH_URL):
        return []

    loaded_data = DataLoader.load_data(data)
    search_results = []
    if type(loaded_data) == d3m_DataFrame:
        q_nodes_columns = []
        metadata_input = loaded_data.metadata
        # check whether Qnode is given in the inputs, if given, use this to wikidata and search
        for i in range(loaded_data.shape[1]):
            metadata_selector = (metadata_base.ALL_ELEMENTS, i)
            if Q_NODE_SEMANTIC_TYPE in metadata_input.query(metadata_selector)["semantic_types"]:
                q_nodes_columns.append(i)

        if len(q_nodes_columns) == 0:
            print("No Wikidata Q nodes inputs detected! Will skip wikidata search part")
        else:
            print("Wikidata Q nodes inputs detected! Will search with it.")
            if len(q_nodes_columns) > 1:
                print("[WARN] More than 1 Q nodes columns detected!")
            for each_column in q_nodes_columns:
                res = search_from_wikidata(loaded_data, each_column, search_threshold)
                if res:
                    search_results.append(res)





def search_from_wikidata(loaded_data: d3m_DataFrame, q_node_column_number: int, search_threshold: float) -> typing.List[str]:
    """
    :param loaded_data: given input DataFrame
    :param q_node_column_number: the column number of q nodes located
    :param search_threshold: the minimum appeared times of the properties
    :return: p_nodes_needed: A list of p_nodes candidates
    """
    q_nodes_list = loaded_data.iloc[:, q_node_column_number].tolist()
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
    return p_nodes_needed


def download_from_wikidata(original_data: d3m_DataFrame,
                           q_node_column_number: int,
                           p_nodes_needed: typing.List[str],
                           generate_metadata=False,
                           ) -> d3m_DataFrame:
    """
    :param original_data: input DataFrame
    :param q_node_column_number: The column number of the q_nodes in input DataFrame
    :param p_nodes_needed: the list of corresponding required property nodes
    :param generate_metadata:  control whether to generate metadata on output DataFrame
    :return: return_df: the materialized wikidata d3m_DataFrame,
                        with corresponding pairing information to original_data at last column
    """
    # prepare the query
    q_nodes_list = original_data.iloc[:, q_node_column_number].tolist()
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
                    get_semantic_type(p_val["datatype"]), 'https://metadata.datadrivendiscovery.org/types/Attribute')
                else:
                    semantic_types_dict[p_name] = (
                    "http://schema.org/Text", 'https://metadata.datadrivendiscovery.org/types/Attribute')

        return_df = return_df.append(each_result, ignore_index=True)

    p_name_dict = {"q_node": "q_node"}
    for each in return_df.columns.tolist():
        if each[0] == "P":
            p_name_dict[each] = get_node_name(each)

    # use rltk joiner to find the joining pairs
    joiner = RLTKJoiner_new()
    joiner.set_join_target_column_name((original_data.columns[q_node_column_number], "q_node"))
    return_df = joiner.find_pair(left_df=original_data, right_df=return_df)

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
            return_df.metadata = return_df.metadata.update(metadata=metadata_each_column, selector=metadata_selector)

        # special for joining_pairs column
        metadata_selector = (ALL_ELEMENTS, return_df.shape[1])
        metadata_joining_pairs = {"name": "joining_pairs", "structural_type": typing.List[int],
                                'semantic_types': ("http://schema.org/Integer",)}
        return_df.metadata = return_df.metadata.update(metadata=metadata_joining_pairs, selector=metadata_selector)

    # update column names to be property names instead of number
    return_df = return_df.rename(columns=p_name_dict)

    return return_df


def get_node_name(node_code):
    sparql_query = "SELECT DISTINCT ?x WHERE \n { \n" + \
      "wd:" + node_code + " rdfs:label ?x .\n FILTER(LANG(?x) = 'en') \n} "
    try:
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

        # sparql = SPARQLWrapper("http://sitaware.isi.edu:8080/bigdata/namespace/wdq/sparql")
        sparql.setQuery(sparql_query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        return results['results']['bindings'][0]['x']['value']
    except:
        print("Getting name of node " + node_code + " failed!")
        return node_code


def get_semantic_type(datatype):
    special_type_dict = {"http://www.w3.org/2001/XMLSchema#dateTime":"http://schema.org/DateTime", "http://www.w3.org/2001/XMLSchema#decimal": "http://schema.org/Float", "http://www.opengis.net/ont/geosparql#wktLiteral": "https://metadata.datadrivendiscovery.org/types/Location"}
    default_type = "http://schema.org/Text"
    if datatype in special_type_dict:
        return special_type_dict[datatype]
    else:
        print("not seen type : ", datatype)
        return default_type


def download(original_data: typing.Union[pd.DataFrame, str, d3m_ds.Dataset],
             augment_data_query: dict,
             joining_columns: typing.Union[typing.Tuple[typing.List[typing.List[typing.Union[str, int]]],
                                                        typing.List[typing.List[typing.Union[str, int]]]]
                                           , None]=None,
             generate_metadata = False,
             ):
    """
    Perform the "download" operation that find the corresponding augment data candidate and pairs for join

    Args:
        original_data: the input data
        joining_columns: user set joining_columns, if not given, will try to find automatically
        augment_data_query: the found candidate dataset from datamart or the query config (with specified datamart id)
        augment_data_query example here:
            {
                "dataset": {
                    "about": "290790000"
                }
            }
    Returns:
        (right_df, join_pairs_return)

        right_df: a pd.DataFrame object which is the meterilized dataFrame
        join_pairs_return: A list of tuples, each tuple contains following things:
        (join_columns, in the shape of "joining_columns", score of the "joining_columns", the detail joining pairs)

    """

    loaded_data = DataLoader.load_data(original_data)
    augmenter = Augment(es_index=PRODUCTION_ES_INDEX)

    # load the query to Dataset if needed
    if type(augment_data_query) is dict:
        try:
            es_result = augmenter.query_by_json(augment_data_query, loaded_data, size=1, return_named_entity=False)
            augment_data = Dataset(es_result[0], original_data=loaded_data, query_json=augment_data_query)
        except Exception as e:
            print("Error when loading the given query", e)
            # return the original dataFrame if query failed
            return JoinResult(loaded_data, [])
    else:
        raise ValueError("The input is not correct")

    candidate_join_column_pairs = []
    join_pairs_result = []
    candidate_join_column_scores = []

    # if target joining columns was given, set up that columns
    if joining_columns:
        candidate_join_column_pairs.append(joining_columns)
        candidate_join_column_scores.append("-1")

    # if target joining columns was not given, try to search all possible combinations
    else:
        for col in loaded_data:
            if Utils.is_column_able_to_query(loaded_data[col]):
                query_copy = augment_data_query.copy()
                query_copy['required_variables'] = [{
                    "type": "dataframe_columns",
                    "names": [col]
                }]
                try:
                    cur_results = augmenter.query_by_json(query_copy, loaded_data,
                                                          size=10,
                                                          return_named_entity=False)
                    for res in cur_results:
                        temp = Dataset(res, original_data=loaded_data, query_json=query_copy)
                        candidate_join_column_pairs.append(temp.join_columns)
                        candidate_join_column_scores.append(temp.score)
                except Exception as e:
                    print("Error when loading the given query", e)

    # start finding pairs
    left_df = loaded_data
    right_metadata = augment_data.metadata
    right_df = augment_data.materialize()
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
                                                            selected_columns=set(chain.from_iterable(right_columns)))
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


def join(left_data: pd.DataFrame or str or d3m_ds.Dataset,
         right_data: Dataset or int or pd.DataFrame or str or d3m_ds.Dataset,
         left_columns: typing.List[typing.List[int or str]],
         right_columns: typing.List[typing.List[int or str]],
         left_meta: dict=None,
         joiner=JoinerType.RLTK
         ) -> JoinResult:
    """

    :param left_data: a tabular data
    :param right_data: a tabular data or the datamart.Dataset(metadata with materialize info)
                        or an int for the datamart_id - Recommend to use datamart.Dataset or ID
    :param left_columns: list of index(indices)/header(headers) for each "key" for joining
    :param right_columns: list of index(indices)/header(headers) for each "key" for joining(same length as left_columns)
    :return: a pandas.DataFrame(joined table)
    """

    if isinstance(right_data, Dataset):
        return augment(left_data, right_data, (left_columns, right_columns), joiner)

    print(" - start loading data")
    left_df = DataLoader.load_data(left_data)
    right_metadata = None
    if isinstance(right_data, int):
        right_metadata, right_df = DataLoader.load_meta_and_data_by_id(right_data)
    else:
        right_df = DataLoader.load_data(right_data)

    if not (isinstance(left_df, pd.DataFrame) and isinstance(right_df, pd.DataFrame) and left_columns and right_columns):
        return JoinResult(left_df, [])

    augmenter = Augment(es_index=PRODUCTION_ES_INDEX)

    print(" - satrt augmenting")
    augmented_data = augmenter.join(
            left_df=left_df,
            right_df=right_df,
            left_columns=left_columns,
            right_columns=right_columns,
            left_metadata=left_meta,
            right_metadata=right_metadata,
            joiner=joiner
    )
    return augmented_data


def get_type(input_val):
    if input_val.isdigit():
        try:
            int(input_val)
            return "int"
        except:
            return "float"
    else:
        return "str"

#
# def download(data, name, destination, url, format:str, timeout=None):
#     """
#
#     :param data: an augmented dataFrame
#     :param data: the saving name of the data
#     :param destination: the target saving location
#     :param url:
#     :param format: if "d3m", save the d3m format dataset (with datasetDoc.json and corresponding metadata inside),
#                     if "pandas", save a csv only
#     :param timeout: timeout for saving
#     :return:
#         file_location: The created file location, if using d3m format, a datasetDoc.json will be pointed
#     """
#     if type(destination) is str:
#         save_location = os.path.join(destination, data+".csv")
#     if format == "pandas":
#         pd.to_csv(save_location)
#     elif format == "d3m":
#         # use inner d3m save function
#         pass
#         save_location = os.path.join(destination, data, "datasetDoc.json")
#     return save_location
#
