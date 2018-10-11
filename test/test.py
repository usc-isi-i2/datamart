import sys, os, json, time, random

sys.path.append(sys.path.append(os.path.join(os.path.dirname(__file__), '..')))
from datamart.query_manager import QueryManager
from datamart.index_builder import IndexBuilder

if __name__ == '__main__':

    # create index and document
    es_index = "datamart_tmp"

    index_builder = IndexBuilder()

    # tmp_description_dir = "/Users/runqishao/Downloads/datamart_data/tmp"
    tmp_description_dir = "./tmp"
    # tmp_out = "/Users/runqishao/Downloads/datamart_data/nooa.metadata"
    tmp_out = "./tmp/nooa.metadata"

    for description in os.listdir(tmp_description_dir):
        if description.endswith('.json'):
            description_path = os.path.join(tmp_description_dir, description)
            print("==== Creating metadata for " + description)
            this_metadata = index_builder.indexing(description_path=description_path,
                                                   es_index=es_index,
                                                   data_path=None,
                                                   query_data_for_indexing=False,
                                                   save_to_file=tmp_out,
                                                   delete_old_es_index=True)


    # index_builder.bulk_indexing(metadata_out_file=tmp_out, es_index=es_index)

    time.sleep(1)

    # query index

    qm = QueryManager()
    query_1 = json.dumps({
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "variables.named_entity": "Los Angeles"
                        }
                    },
                    {
                        "match": {
                            "variables.named_entity": "new york"
                        }
                    },
                    {
                        "match": {
                            "description": "average"
                        }
                    }
                ]
            }
        }
    })

    hitted_metadatas = qm.search(index=es_index, body=query_1)

    print(len(hitted_metadatas))
    # materialize data
    for metadata in hitted_metadatas:
        print("====== HIT a metadata ======")
        print("\n")
        print("====== GET the dataset ======")
        df = qm.get_dataset(metadata=metadata, variables=None, constrains={
            "locations": ["los angeles", "new york"],
            "date_range": {
                "start_date": "2018-09-23T00:00:00",
                "end_date": "2018-10-01T00:00:00"
            }
        })
        print(df.iloc[random.sample(range(1, df.shape[0]), 10), :])
        print("\n\n")
