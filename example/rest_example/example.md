
### ISI-datamart Restful API

#### There is a service running on dsbox02.isi.edu:9000

#### RESTful APIs - Basics:

##### 1. search
- route: `/new/search_data`
- methods: `POST`
- body`form-data`: two files, one for query json, the other for supplied data(csv, optional)
    - `query`: file, json file following [query schema](https://datadrivendiscovery.org/wiki/pages/viewpage.action?spaceKey=work&title=Datamart+Query+API)
    - `data`: file, csv file (optional)
- params: 
    - `max_return_docs`: a number for the maximum number of search results will be returned(default is 10)
    - `return_named_entity`: `true` or `false` for if return the `named_entity` (default is false)
- example:
    ```angular2html
    curl -X POST \
      https://dsbox02.isi.edu:9000/new/search_data 
      -H 'content-type: multipart/form-data' \
      -F data=@datamart/example/fifa_example/fifa.csv \
      -F query=@datamart/example/fifa_example/fifa_query.json
    ```
- [sample response](search_response.json)
    ```angular2html
    {
      "code": "0000",
      "message": "Success",
      "data": [
        {
          "summary": "STRING SUMMARY FOE THE DATASET"
          "score": 84.735825,
          "metadata": {},
          "datamart_id": "127860000"
        }, 
        ...
      ]
    }
    ```
    
##### 2. materialize
- route: `/new/materialize_data`
- methods: `GET`
- params: one param for datamart_id
    - `datamart_id`: the datamart_id of the data you would like to materialize
- example:
    ```angular2html
    curl -X GET \
      'https://dsbox02.isi.edu:9000/new/materialize_data?datamart_id=127860000'
    ```
- [sample response](materialize_response.json)
    ```angular2html
    {
      "code": "0000",
      "message": "Success",
      "data": "CSV RESULT HERE"
    }
    ```

    
##### 3. join(augment)
- route: `/new/join_data`
- methods: `POST`
- body`form-data`: one file for supplied dataset, one field for datamart_id of augment data, two fields for the joining columns
    - `left_data`: file, a csv file, which is the supplied data provided by users
    - `right_data`: text, a datamart_id for the data you would like to use for augmentation
    - `left_columns`: text, specify the join features in the left dataset, by column indeces
    - `right_columns`: text, specify the join features in the right dataset, by column indeces
- example:
    ```angular2html
    curl -X POST \
      https://dsbox02.isi.edu:9000/new/join_data \
      -H 'content-type: multipart/form-data' \
      -F left_data=@datamart/example/fifa_example/fifa.csv \
      -F right_data=127860000 \
      -F 'left_columns=[[3], [4]]' \
      -F 'right_columns=[[22], [24]]'
    ```
- [sample response](materialize_response.json)
    ```angular2html
    {
      "code": "0000",
      "message": "Success",
      "data": "CSV RESULT HERE"
    }
    ```
    
#### Upload data to ISI-datamart:

If you would like to index a new dataset into ISI-datamart, there are two methods:
1. By single file:
    1. find the url for the data you would like to upload:
        - it can be a csv file, an excel file, an html page with tabular data.
    2. construct a description json for the data like:
        ```angular2html
        {
           "title": "title for the dataset", 
           "description": "the description for the dataset",
           ...
           "materialization_arguments": {
               "url": "http://example.com/sample_csv.csv",
               "file_type": "csv"
           }
        }
        ```
        - The only __required__ field is `materialization_arguments.url`, all the others are optional.
        - More available attributes can be found in [index_schema](../../datamart/resources/index_schema.json)
    3. call the `/new/get_metadata_single_file` api with the description json, and check the returned metadata
    4. send the confirmed metadata through `/new/upload_metadata_list` to finish indexing

2. By a html page that includes many links for "single file"
    1. find the url for the html, containing many links for dataset you would like to upload
        - ISI-datamart will extract <a> tags and recognize if the link is a data file 
        - if so, try to materialize each file and generate metadata
    2. call `/new/get_metadata_extract_links` with the url in body json and check the returned metadata
    3. send the confirmed metadata through `/new/upload_metadata_list` to finish indexing

#### RESTful APIs - Upload data:
##### 1. get metadata for a single file
- route: `/new/get_metadata_single_file`
- methods: `POST`
- body`json`: the description json for the file, including the url 
    - see 1.ii above
- example:
    ```angular2html
    curl -X POST \
      https://dsbox02.isi.edu:9000/new/get_metadata_single_file \
      -H 'Content-Type: application/json' \
      -d '{
        "materialization_arguments": {
            "url": "https://www.w3schools.com/html/html_tables.asp",
            "file_type": "html"
        }
      }'
    ```
- sample response:
    ```angular2html
    {
      "code": "0000",
      "message": "Success",
      "data": [   // a list of metadata object, mostly only one metadata in the list
          {},     // when there are many sheets in an excel file there can be mutiple matadata
        ...
       ]
    }
    ```
    
##### 2. get metadata by link extraction from an HTML page
- route: `/new/get_metadata_extract_links`
- methods: `POST`
- body`json`: `{"url": "http://example.page.with.many.csv.links"}`
- example:
    ```angular2html
    curl -X POST \
      https://dsbox02.isi.edu:9000/new/get_metadata_extract_links \
      -H 'Content-Type: application/json' \
      -d '{
        "url": "https://sample-videos.com/download-sample-xls.php"
    }'
    ```
- sample response:
    ```angular2html
    {
      "code": "0000",
      "message": "Success",
      "data": [
          // each inner-list is for a link:
          [   // a list of metadata object, mostly only one metadata in the list
              {},     // when there are many sheets in an excel file there can be mutiple matadata
            ...
          ],
          [],
          ...
        ]
    }
    ```
    
##### 3。 upload metadata to ISI-datamart
- route: `/new/upload_n=metadata_list`
- methods: `POST`
- body`json`: hold the metadata(or list(metadata), list(list(metadata)))
    - `metadata`: put the metadata(or list(metadata), list(list(metadata)))
- example:
    ```angular2html
    curl -X POST \
      https://dsbox02.isi.edu:9000/new/upload_metadata_list \
      -H 'Content-Type: application/json' \
      -d '{
        "metadata":     {
          "datamart_id": 0,
          "title": "html tables",
          "url": "https://www.w3schools.com/html/html_tables.asp",
          "materialization": {
            "python_path": "general_materializer",
            "arguments": {
              "url": "https://www.w3schools.com/html/html_tables.asp",
              "file_type": "html",
              "index": 0
            }
          },
          "variables": [
            {
              "datamart_id": 1,
              "semantic_type": [],
              "name": "Company",
              "description": "column name: Company, dtype: object",
              "named_entity": [
                "Alfreds Futterkiste",
                "Centro comercial Moctezuma",
                "Ernst Handel",
                "Island Trading",
                "Laughing Bacchus Winecellars",
                "Magazzini Alimentari Riuniti"
              ]
            },
            {
              "datamart_id": 2,
              "semantic_type": [],
              "name": "Contact",
              "description": "column name: Contact, dtype: object",
              "named_entity": [
                "Maria Anders",
                "Francisco Chang",
                "Roland Mendel",
                "Helen Bennett",
                "Yoshi Tannamuri",
                "Giovanni Rovelli"
              ]
            },
            {
              "datamart_id": 3,
              "semantic_type": [],
              "name": "Country",
              "description": "column name: Country, dtype: object",
              "named_entity": [
                "Germany",
                "Mexico",
                "Austria",
                "UK",
                "Canada",
                "Italy"
              ]
            }
          ],
          "description": "Company : object, Contact : object, Country : object",
          "keywords": [
            "Company",
            "Contact",
            "Country"
          ]
        }
    }'
    ```
- sample response:
    ```angular2html
    {
      "code": "0000",
      "message": "Success",
      "data": [   // successed metadata, with valid datamart_id assigned
          {},
        ...
       ]
    }
    ```


#### You can run your own flask
```commandline
conda activate datamart_env
python ../../datamart_web/webapp.py
```
