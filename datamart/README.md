### Datamart API

### There are three main APIs:
1. datamart.search(query: dict, data: pandas.DataFrame) -> list[datamart.Dataset]
  - input:
    - a description json object for the target datasets
    - the original dataset to be augmented
  - output: 
    - a list of datamart.Dataset objects, each is for a dataset indexed in Datamart
    
2. datamart.augment(original_data: pandas.DataFrame, augment_data: datamart.Dataset) -> pandas.DataFrame
  - input:
    - the original dataset to be augmented
    - the datamart.Dataset to be used for augmentation
  - output:
    - the augmented data
    
3. datamart.upload(description: dict, es_index: str=None) -> dict
  - input:
    - a description json for the dataset, including the url for the concrete data(e.g. an url for a csv file)
    - where to index(OPTIONAL) - used to toggle test/in-use datamart ES indices
  - output:
    - the final object indexed into datamart(with user provided description, inferred description, profiling info etc.)
