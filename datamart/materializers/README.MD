## How to write a materializer

1. Create a subclass of [MaterializerBase](./materializer_base.py)

2. Implement `get(self, metadata: dict = None, constrains: dict = None) -> pd.DataFrame` method,
    
    metadata is the metadata after processing your dataset schema (profiling and so on).
    
    The`materialization` field will not be changed, so put arguments you need 
    for materializing your dataset at `materialization.arguments` in any format
 
    constrains, the constrains for query your datasets, it only contains
    `named_entity` and `date_range`. By default, it should return entire dataset
    
    Assume you will get constrains in the following format
    ```
    constrains={
        # constrain by named entity if any column of your dataset is about named entity
        "named_entity": {
            column_index: list of named entities to be in that column
        },
        "date_range": {
            "start": string in yyyy-mm-ddTHH:MM:SS,
            "end": string in yyyy-mm-ddTHH:MM:SS,
        }
    }
    ```
    Eg.
    ```
    constrains={
        "named_entity": {
            2 : ["los angeles"]
        },
        "date_range": {
            "start": "2018-09-23T00:00:00",
            "end": "2018-10-01T00:00:00"
        }
    }
    ```
    returns a dataframe
   
    take a look at [noaa_materializer.py](./datamart/materializers/noaa_materializer.py) for example.
    
