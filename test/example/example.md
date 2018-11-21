### Example of querying current to augment a fake taxi example

#### You can run your own flask
```commandline
conda activate datamart_env
python ../../datamart_web/webapp.py
```

#### There is a service running on dsbox02
#### Fake the `taxi` example by add additional column, input data is [fake_taxi.csv](./fake_taxi.csv)

POST a query to default search for metadatas with the input dataset, along with a query string.

The following curl using `wind` as query string, write to `result.json`

```commandline
curl -X POST 'http://dsbox02.isi.edu:5000/search/default_search?query_string=wind' -F "file=@{your_path}/test_fake.csv" > result.json
```

It should return many metadata hitted.

Suppose user select one, front end will return some request like [sample_query.json](./sample_query.json)

`selected_metadata` is the metadata user selected

`columns_mapping` is the mapping used for joiner, telling pairs of columns for join


Second request is for default join

We get the data from noaa api by querying the matched named_entity from provided dataset.
And time range from the provided dataset as well

Otherwise noaa data is too big to return.

Then join two dataframes using default outer join
```commandline
value=$(<{your_path}/sample_query.json); curl -X POST  http://dsbox02:5000/augment/default_join -F "data=$value" > augmented.csv
```

It returns a csv `augmented.csv` now
