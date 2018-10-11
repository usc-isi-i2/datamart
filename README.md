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

## How to provide index for one dataset source

1. Prepare your dataset schema and validate it with the previous step

2. Create your materialization method and put in `datamart/materializers`,
take a look at `datamart/materializers/noaa_materializer.py` for example.

3. Have your dataset schema json pointed to the materialization method.
Take a look at `test/tmp/tmp.json`.


4. Try to create metadata and index it on Elasticsearch, following: [Indexing demo](./test/indexing.ipynb)

5. Try some queries for testing you materialization method, following: [Query demo](./test/query.ipynb)


Note: Launch notebook: 
```
jupyter notebook test/indexing.ipynb
```