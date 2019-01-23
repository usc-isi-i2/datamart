import pandas as pd
import typing
from datamart.dataset import Dataset
from datamart.index_builder import IndexBuilder
from datamart.utilities.utils import DEFAULT_ES
from datamart.augment import Augment
from datamart.data_loader import DataLoader
import d3m.container.dataset as d3m_ds
from datamart.utilities.utils import Utils


def search(url: str, query: dict, data: pd.DataFrame or str or d3m_ds.Dataset=None, send_data=True) -> typing.List[Dataset]:
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
    if url != 'isi-datamart':
        return []

    loaded_data = DataLoader.load_data(data)
    augmenter = Augment(es_index=DEFAULT_ES)

    es_results = []
    if (query and ('required_variables' in query)) or (loaded_data is None):
        # if ("required_variables" exists or no data):
        es_results = augmenter.query_by_json(query, loaded_data)
    else:
        # if there is no "required_variables" in the query JSON, but the dataset exists,
        # try each named entity column as "required_variables" and concat the results:
        query = query or {}
        for col in loaded_data:
            if Utils.is_column_able_to_query(loaded_data[col]):
                query['required_variables'] = [{
                    "type": "dataframe_columns",
                    "names": [col]
                }]
                for res in augmenter.query_by_json(query, loaded_data):
                    es_results.append(res)
    return [Dataset(es_result, original_data=loaded_data, query_json=query) for es_result in es_results]


def augment(original_data: pd.DataFrame or str or d3m_ds.Dataset,
            augment_data: Dataset,
            joining_columns: typing.Tuple[typing.List[typing.List[int or str]]]=None) -> pd.DataFrame:
    """
    Perform the augmentation (either join or union).
    Follow the API defined by https://datadrivendiscovery.org/wiki/display/work/Python+API

    Args:
        original_data:
        augment_data:
        joining_columns: user defined which columns to be joined

    Returns:

    """

    loaded_data = DataLoader.load_data(original_data)
    if joining_columns:
        try:
            augment_data.set_join_columns(*joining_columns)
        except Exception as e:
            print("FAILED SET JOINING COLUMNS:", e)

    if not augment_data.join_columns:
        return loaded_data

    left_cols, right_cols = augment_data.join_columns
    default_joiner = 'rltk'
    augmenter = Augment(es_index=DEFAULT_ES)

    augmented_data = augmenter.join(
            left_df=loaded_data,
            right_df=augment_data.materialize(),
            left_columns=left_cols,
            right_columns=right_cols,
            left_metadata=None,
            right_metadata=augment_data.metadata,
            joiner=default_joiner
    )
    return augmented_data


def upload(description: dict, es_index: str=None) -> dict:
    """

    Args:
        description:

    Returns:

    """

    description['materialization'] = {
        'python_path': 'general_materializer',
        'arguments': description['materialization_arguments']
    }
    del description['materialization_arguments']
    ib = IndexBuilder()
    metadata = ib.indexing(description_path=description, es_index=es_index or DEFAULT_ES, query_data_for_indexing=True)

    return metadata