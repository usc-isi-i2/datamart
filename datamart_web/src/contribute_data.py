from datamart.index_builder import IndexBuilder


class UserDataIndexer(object):
    def __init__(self):
        self.ib = IndexBuilder()

    def index(self, description: dict, es_index: str):
        return self.ib.indexing(
                 description_path=description,
                 es_index=es_index,
                 query_data_for_indexing=True)
