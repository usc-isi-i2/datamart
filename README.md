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

1. Prepare your dataset schema following [datamart index schema](https://paper.dropbox.com/doc/Datamart-Index-Schema--ARZ9ANxCYpvOOfTKxXGE9MI1Ag-0Uu03rDIUCttwS0x9GLCq)
 and validate it with the previous step

2. Create your materialization method by creating a subclass of [`materializer_base.py`](./datamart/materializers/materializer_base.py).
and put in `datamart/materializers`. See [README](./datamart/materializers/README.MD)

3. Have your dataset schema json `materialization.python_path` pointed to the materialization method. 
Take a look at [tmp.json](./test/tmp/tmp.json#L10).

4. Play with the following:

## Example of using current system

#### Create metadata and index it on Elasticsearch, following: [Indexing demo](./test/index.ipynb)
#### Query datamart, following: [Query demo](./test/query.ipynb)
#### Dealing with TAXI example, following: [taxi_example](./test/taxi_example.ipynb)


Note: Launch notebook: 
```
jupyter notebook test/index.ipynb
```
