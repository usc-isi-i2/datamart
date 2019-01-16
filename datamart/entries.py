import pandas as pd
import typing
from datamart.dataset import Dataset
from datamart.index_builder import IndexBuilder
from datamart.utilities.utils import DEFAULT_ES
from datamart.augment import Augment
from datamart.data_loader import DataLoader
import d3m.container.dataset as d3m_ds
from datamart.utilities.utils import Utils


def search(query: dict, data: pd.DataFrame or str or d3m_ds.Dataset =None) -> typing.List[Dataset]:
    """
    Follow the API defined by https://datadrivendiscovery.org/wiki/display/work/Python+API

    Args:
        query: JSON object describing the query(https://datadrivendiscovery.org/wiki/display/work/Query+results+schema)
        data: the data you are trying to augment. It can be provided as one of:
            - a pandas.DataFrame object
            - a D3M Dataset object
            - the path to a D3M datasetDoc.json file
            - the path to a CSV file

    Returns: a list of datamart.Dataset objects.

    """
    loaded_data = DataLoader.load_data(data)
    augmenter = Augment(es_index=DEFAULT_ES)
    if not (query and ('required_variables' in query)) and (loaded_data is not None):
        query = query or {}
        query['required_variables'] = []
        for col in loaded_data:
            if Utils.is_column_able_to_query(loaded_data[col]):
                query['required_variables'].append({
                    "type": "dataframe_columns",
                    "names": [
                      col
                    ]
                })
    es_results = augmenter.query_by_json(query, loaded_data)
    if es_results:
        return [Dataset(es_result, original_data=loaded_data, query_json=query) for es_result in es_results]
    return []


def augment(original_data: pd.DataFrame or str or d3m_ds.Dataset, augment_data: Dataset) -> pd.DataFrame:
    """
    Perform the augmentation (either join or union).
    Follow the API defined by https://datadrivendiscovery.org/wiki/display/work/Python+API

    Args:
        original_data:
        augment_data:

    Returns:

    """

    loaded_data = DataLoader.load_data(original_data)

    if not augment_data.matched_cols:
        return loaded_data

    left_cols, right_cols = augment_data.matched_cols
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