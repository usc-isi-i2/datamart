# datamart

```commandline
conda env create -f environment.yml
source activate datamart_env
```


## test example

There is a test example in `test` dir

`tmp` contains two sample json files I got from zhihao, the student who works on
NOAA. 

`test.py` is script running the whole workflow. 

1. Create metadata json file, by reading the json description file and profile
the original dataset (query to get original dataset, current profiler only record name_entity column). 

2. Load to elasticsearch and index metadata (using default setting now).

3. Query the index with a simple query which try to find `los angeles` in 
any `name_entity` column.

4. Materialize data.

```commandline
cd test
python test.py
```

Metadata json file will be in `tmp/tmp_metadata.out`
