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

## Workflow example

#### `demo.ipynb` contains a step by step demo.
```commandline
jupyter notebook test/demo.ipynb
```
