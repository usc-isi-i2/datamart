import sys, os, json, time
sys.path.append(sys.path.append(os.path.join(os.path.dirname(__file__), '..')))
from datamart.index_builder import IndexBuilder
from datamart.query_manager import QueryManager


if __name__ == '__main__':

    # create index
    es_index = "datamart_tmp"

    index_builder = IndexBuilder()
    qm = QueryManager()

    tmp_description_dir = "./tmp"

    tmp_out = "./tmp/tmp_metadata.out"

    if qm.check_exists(index=es_index):
        qm.delete_index(index=[es_index])
    qm.create_index(index=es_index)

    for description in os.listdir(tmp_description_dir):
        if description.endswith('.json'):
            description_path = os.path.join(tmp_description_dir, description)
            this_metadata = index_builder.indexing(description_path=description_path,
                                                   data_path=None,
                                                   query_data_for_indexing=True,
                                                   save_to_file=tmp_out)
            # qm.create_doc(index='datamart_tmp', doc_type='document', body=this_metadata, id=this_metadata['datamart_id'])

    index_builder.save_index_config()

    # query index

    qm.create_doc_bulk(file=tmp_out, index=es_index)

    time.sleep(1)

    query_1 = json.dumps({
        "query": {
            "bool": {
              "must": [
                {
                  "match": {
                    "variables.named_entity": "los angeles"
                  }
                }
              ]
            }
        }
    })

    hitted_metadatas = qm.search(index=es_index, body=query_1)

    # materialize data
    for metadata in hitted_metadatas:
        print("====== HIT a metadata ======")
        print(metadata)
        print("\n\n")
        print("====== GET the dataset ======")
        df = qm.get_dataset(metadata=metadata)
        print(df)
        print("\n\n")
