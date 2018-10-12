![MIT License](https://img.shields.io/badge/license-MIT-blue.svg) ![travis ci](https://travis-ci.org/usc-isi-i2/etk.svg?branch=master)

# datamart

```commandline
cd datamart
conda env create -f environment.yml
source activate datamart_env
git update-index --assume-unchanged datamart/resources/index_info.json

python -W ignore -m unittest discover
```

## Validate your schema
Dataset providers should validate their dataset schema against our json schema by the following
```commandline
python scripts/validate_schema.py --validate_json {path_to_json}
```
eg.
```commandline
$ python scripts/validate_schema.py --validate_json test/tmp/tmp.json
$ Valid json
```

## How to provide index for one data source

1. Prepare your dataset schema and validate it with the previous step

2. Create your materialization method by creating a subclass of [`materializer_base.py`](./datamart/materializers/materializer_base.py).
and put in `datamart/materializers`.

    Implement `get(self, metadata: dict = None, variables: typing.List[int] = None, constrains: dict = None) -> pd.DataFrame` method,
    
    metadata is the metadata after processing your dataset schema (profiling and so on).
    `materialization` field will not be changed, so put every arguments you need for materializing your dataset at `materialization.arguments`
    
    variables, default to None for now
    
    constrains, fake some constrains for your dataset for querying, eg. 
    ```
    constrains={
        "locations": ["los angeles", "new york"],
        "date_range": {
            "start": "2018-09-23T00:00:00",
            "end": "2018-10-01T00:00:00"
        }
    }
    ```
    
    returns a dataframe
   
    take a look at [noaa_materializer.py](./datamart/materializers/noaa_materializer.py) for example.

3. Have your dataset schema json `materialization.python_path` pointed to the materialization method. 
Take a look at [tmp.json](./test/tmp/tmp.json#L10).

4. Try to create metadata and index it on Elasticsearch, following: [Indexing demo](./test/indexing.ipynb)

5. Try some queries for testing you materialization method, following: [Query demo](./test/query.ipynb)


Note: Launch notebook: 
```
jupyter notebook test/indexing.ipynb
```
