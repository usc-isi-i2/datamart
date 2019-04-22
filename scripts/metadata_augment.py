from elasticsearch import Elasticsearch
from datamart.index_builder import IndexBuilder
import math
import traceback
"""
A function used to do keywords augmentation
"""
def update(url="http://dsbox02.isi.edu:9200/", index="", size=1000):
    es = Elasticsearch([url])
    # es = Elasticsearch(['http://dsbox02.isi.edu:9200/datamart_v2/'])
    doc = {
            'size' : size,
            'query': {
                'match_all' : {}
           }
       }
    
    scroll_times = math.ceil(es.count()['count'] / size)

    succeeded_count_all = 0
    failed_count_all = 0
    no_keywords_all = 0
    a = IndexBuilder()

    for i in range(scroll_times):
        # first time, just serach
        if i == 0:
            res = es.search(index=index,doc_type='_doc',body=doc,scroll='10m')
        else:
            # otherwise just scroll
            scrollId = res['_scroll_id']
            res = es.scroll(scroll_id = scrollId, scroll = '10m')

        datasets = res['hits']['hits']
        print("processing " + str(i) + "000 to " + str(i+1) + "000")
        succeeded_count = 0
        failed_count = 0
        no_keywords_count = 0
        for each in datasets:
            try:
                if "keywords" in each['_source']:
                    new_res = IndexBuilder.augment_metadata(each['_source'])
                    # print(new_res["keywords"])
                    # print(new_res["augmented_keywords"])
                    a.im.update_doc(index=index, doc_type='_doc', body={"doc": new_res}, id=new_res['datamart_id'])
                    # print("augmenting on dataset " + each['_id'] + " succeeded")
                    if "augmented_keywords" in new_res:
                        succeeded_count += 1
                    else:
                        failed_count += 1
                else:
                    print("no keywords found")
                    no_keywords_count += 1
            except:
                failed_count += 1
                print("augmenting on dataset " + each['_id'] + " failed")
                traceback.print_exc()

            succeeded_count_all += succeeded_count
            failed_count_all += failed_count
            no_keywords_all += no_keywords_count
        print("conclusion: succeeded : " + str(succeeded_count) + ", failed : " + str(failed_count) + ", no_keywords : " + str(no_keywords_count))
    print("*" * 100)
    print("Finished!")
    print("conclusion: \nsucceeded : " + str(succeeded_count_all) + "\nfailed : " + str(failed_count_all) + "\nno keywords : " + str(no_keywords_all))

if __name__ == '__main__':
    update(index = "datamart_v2")