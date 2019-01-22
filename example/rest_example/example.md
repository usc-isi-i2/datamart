# Please switch to the [development branch](https://github.com/usc-isi-i2/datamart/tree/development) for the latest APIs.

--------------------

### Example of querying current rest service to augment a fake taxi example dataset

#### You can run your own flask
```commandline
conda activate datamart_env
python ../../datamart_web/webapp.py
```

#### There is a service running on dsbox02.isi.edu:9001

#### RESTful APIs:
##### 1. Upload:
```angular2html
curl -X POST \
  https://dsbox02.isi.edu:9001/new/upload_data \
  -H 'content-type: multipart/form-data' \
  -F file=@/path/to/your/description/json/file/example.json \
  -F test=true
```
**Use `test=true` for test and `test=false` to index into the in-use datamart**
- Request Method: POST
- Endpoint: https://dsbox02.isi.edu:9001/new/upload_data
- Body: 
    - file : the description json file for the dataset [(example input: upload.json)](upload.json)
    - test : either `true` or `false`, decide where to index the data(test of in-use)
- Response:
    - response.code: 0000 for success
    - response.message
    - response.data: the metadata json object that was indexed into ES
    
##### 2. Search:
```angular2html
curl -X POST \
  https://dsbox02.isi.edu:9001/new/search_data \
  -H 'content-type: multipart/form-data' \
  -F query=@/path/to/your/query/json/file/example.json \
  -F data=@/path/to/your/supply/data/file/example.csv
```
- Request Method: POST
- Endpoint: https://dsbox02.isi.edu:9001/new/search_data
- Body: 
    - query : the query json file [(example input: query.json)](query.json)
    - data : the data csv file [(example input: fifa.csv)](fifa.csv)
- Response:
    - response.code: 0000 for success
    - response.message
    - response.data: an array of results(ElasticSearch returned json)
    
    
##### 3. Mateiralize a search results:
**"Mateiralize" only works after a successful "Search" request**
```angular2html
curl -X GET 'https://dsbox02.isi.edu:9001/new/materialize_data?index=0' 
```
- Request Method: GET
- Endpoint: https://dsbox02.isi.edu:9001/new/materialize_data
- Param: 
    - index :  the index of the dataset you want to materialize, in the search result
- Response:
    - response.code: 0000 for success
    - response.message
    - response.data: data in string(csv)

