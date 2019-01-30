from d3m.container.dataset import Dataset
import pandas as pd
import os
import typing
from datamart.utilities.utils import Utils, ES_PORT, ES_HOST, PRODUCTION_ES_INDEX
from datamart.es_managers.query_manager import QueryManager


class DataLoader:
    @classmethod
    def load_data(cls, data: pd.DataFrame or Dataset or str) -> typing.Optional[pd.DataFrame]:
        if isinstance(data, pd.DataFrame):
            return data
        if isinstance(data, Dataset):
            return cls.load_dataset(data)
        if isinstance(data, str):
            if data.endswith('.csv'):
                return pd.read_csv(data)
            else:
                return cls.load_dataset(Dataset.load(
                    'file://{dataset_doc_path}'.format(dataset_doc_path=os.path.abspath(data))))

    @classmethod
    def load_dataset(cls, d3m_dataset: Dataset) -> typing.Optional[pd.DataFrame]:
        entry_id = '0'
        for resource_id in d3m_dataset.keys():
            if "https://metadata.datadrivendiscovery.org/types/DatasetEntryPoint" in \
                    d3m_dataset.metadata.query((resource_id,))['semantic_types']:
                entry_id = resource_id
                break
        if isinstance(d3m_dataset[entry_id], pd.DataFrame):
            return d3m_dataset[entry_id]

    @staticmethod
    def load_meta_and_data_by_id(datamart_id: int, first_n_rows: int=None):
        qm = QueryManager(es_host=ES_HOST, es_port=ES_PORT, es_index=PRODUCTION_ES_INDEX)
        res = qm.get_by_id(datamart_id)
        if res and res.get('_source'):
            df = Utils.get_dataset(res['_source'])
            if first_n_rows:
                df = df.head(first_n_rows)
            return res['_source'], df
        return None, None

