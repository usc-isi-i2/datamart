from d3m.container.dataset import Dataset
import pandas as pd
import os
import typing


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
