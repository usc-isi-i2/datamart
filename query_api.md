Query API

# Query API
### 1. Input
- Dataframe: a d3m dataframe
- query: a JSON object representing what the user what to query(See [2. Query Schema](#2.-Query-Schema))

### 2. Query Schema
Domain-specific language to specify queries for searching datasets in Datamart.
- Descriptions:
  The `query` will be a JSON object, with three root properties: `dataset`, `required_variables`(optional) and `desired_variables`(optional).
  - `dataset`: contains global information of the desired datasets, like what the dataset is about, when the dataset is published etc., with the following fields:
    - `about`: a query __string__ that is matched with all information in a dataset, including all dataset and column metadata and all values. A matching dataset should match at least one of the words in the query string. The matching algorithm gives preference to phrases when possible.
    - `name`: an __array of string__ of the names/titles of a dataset. (http://schema.org/name)
    - `description`: an __array of string__ of the descriptions of a dataset. (http://schema.org/description)
    - `keywords`: an __array of string__ of the keywords of a dataset. (http://schema.org/keywords)
    - `creator`: an __array of string__ of the creators of a dataset. (http://schema.org/creator)
    - `publisher`: an __array of string__ of the publishers of a dataset. (http://schema.org/publisher)
    - `date_created`: an __object__ define when a dataset is created, with fields `after` and `before`, each is a __string__ for a date. (http://schema.org/dateCreated) 
        _(inclusive for both `after` and `before`)_.
    - `date_published`: an __object__ define when a dataset is published, with fields `after` and `before`, each is a __string__ for a date. (http://schema.org/datePublished) 
        _(inclusive for both `after` and `before`)_.
    - `url`: an __array of string__ of the URLs of a dataset. (http://schema.org/url)
  - `required_variables`(optional): contains an __array of object__, each object will represent a [varaible](#*variable) that is required in the matching datasets. All variables in the 'required_variables' set must be match by at least one column in a matching dataset. It is possible that an item is matched using a combination of columns. For example, a temporal item with day resolution can be matched by a dataset that represents dates using multiple columns, for year, month and date.  Typically, the 'required_variables' section is used to list columns to be used to perform a join. 
  - `desired_variables`(optional): contains an __array of object__, each object will represent a [varaible](#*variable) that is desired in the matching datasets. The 'desired_variables' section describes the minimum set of columns that a matching dataset must have. A matching dataset must contain columns that match at least one of the 'desired_variables'. Typically, the 'desired_variables' are used to specify columns that will be used for augmentation.
  - *variable: an __object__, with a required key `type` whose value is one of [`temporal_entity`, `geospatial_entity`, `dataframe_columns`, `generic_entity`]:
    1. `temporal_entity`: describe columns containing temporal information. 
          - `type`: "temporal_entity"
          - `start`: a string for date(time), requested dates are equal or older than this date.
          - `end`: a string for date(time), requested dates are equal or more recent than this date.
          - `granularity`: enum, one of ["year", "month", "day", "hour", "minute", "second"], requested dates are well matched with the requested granularity. For example, if "day" is requested, the best match is a dataset with dates; however a dataset with hours is relevant too as hourly data can be aggregated into days.
    2. `geospatial_entity`: describe columns containing geospatial entities such as cities, countries, etc.
          - `type`: "geospatial_entity"
          - `circle`: object. Geospatial circle area identified using a radius and a center point on the surface of the earth.
            - `latitude`: number. The latitude of the center point
            - `longitude`: number. The longitude of the center point
            - `radius`: string. A string specify the radius of the area.
            - `granularity`: string(one of ["country", "state", "city", "county", "postalcode"]). The granularity of the entities contained in a bounding box.
          - `bounding_box`: object. Geospatial bounding box identified using two points on the surface of the earth.
            - `latitude1`: number. The latitude of the first point
            - `longitude1`: number. The longitude of the first point
            - `latitude2`: number. The latitude of the second point
            - `longitude2`: number. The longitude of the second point
            - `granularity`: string(one of ["country", "state", "city", "county", "postalcode"]). The granularity of the entities contained in a bounding box.
          - `named_entities`: object. A set of names of geospatial entities. This should be used when the requestor doesn't know what type of geospatial entities are provided, they could be cities, states, countries, etc. A matching dataset should have a column containing the requested entities.
            - `semantic_type`: string(one of ["http://schema.org/AdministrativeArea", "http://schema.org/Country", "http://schema.org/City", "http://schema.org/State"]).
            - `items`: array.
    3. `dataframe_columns`: describe columns that a matching dataset should have in terms of columns of a known dataframe. 
          - `type`: "dataframe_columns"
          - `index`: array. A set of indices that identifies a set of columns in the known dataset. When multiple indices are provides, the matching dataset should contain columns corresponding to each of the given columns.
          - `names`: array. A set of column headers that identifies a set of columns in the known dataset. When multiple headers are provides, the matching dataset should contain columns corresponding to each of the given columns.
          - `relationship`: string(one of ["contains", "similar", "correlated", "anti-correlated", "mutually-informative", "mutually-uninformative"]). The relationship between a column in the known dataset and a column in a matching dataset. The default is 'contains'.
    4. `generic_entity`: describe any entity that is not temporal or geospatial. Temporal and geospatial entities receive special treatment. Datamart can re-aggregate and disaggregate temporal and geo-spatial entities so that the granularity of the requested data and an existing dataset does not need to match exactly.
          - `type`: "generic_entity"
          - `about`: string. A query sting that is matched with all information contained in a column including metadata and values. A matching dataset should contain a column whose metadata or values matches at least one of the words in the query string. The matching algorithm gives preference to phrases when possible.
          - `variable_name`: array. A set of header names. A matching dataset should have a column that matches closely one of the provided names.
          - `variable_metadata`: array. A set of keywords to be matched with all the words appearing in the metadata of a column. A matching dataset should contain a column whose metadata matches at least one of the keywords.
          - `variable_description`: array. A set of keywords to be matched with all the words in the description of a column in a dataset. A matching dataset should contain a column whose description matches at least one of the keywords.
          - `variable_syntactic_type`: array. A set of syntactic types. A matching dataset should contain a column with any of the provided syntactic types. Comment: this should be defined using an enum.
          - `variable_semantic_type`: array. A set of semantic types. A matching dataset should contain a column whose semantic types have a non empty intersection with the provided semantic types.
          - `named_entities`: array. A set of entity names. A matching dataset shold contain a column with the requested names.
          - `column_values`: object.
            - `items`: array. A set of arbitrary values of any type, string, number, date, etc. To be used with the caller doesn't know whether the values represent named entities. A matching dataset shold contain a column with the requested values.
            - `relationship`: string(one of ["contains", "similar", "correlated", "anti-correlated", "mutually-informative", "mutually-uninformative"]). The relationship between the specified values and the values in a column in a matching dataset. The default is "contains".
- [Detailed sample query](https://www.dropbox.com/s/cg87gsmfoh6k82x/query-detail-isi-02.json?dl=0)
- [Real examples](https://www.dropbox.com/s/bzofafmlg1v3xh3/Sample_query_Taxi_FIFA_HOF-02.json?dl=0)
- [JSON Schema](https://www.dropbox.com/s/txuj2fkbqtmh3m1/query-schema-isi-02.json?dl=0)

### 3. Output
A list of query results, each represents a dataset by `metadata`,  `required_variables`, `desired_variables`, `other_variables` and a ranking `score`.
- `metadata`: an object, d3m metadata? (has the metadata for the matching dataset on global level, as well as the metadata for each variable.)
- `required_variables`: an array of variable names in the dataset which are matched to the `required_variables` in the query.
- `desired_variables`: an array of variable names in the dataset which are matched to the `desired_variables` in the query.
- `other_variables`: an array of variable names which are in the matching dataset but not in `required_variables` or `desired_variables` of the query.
- `score`: a number for how well the dataset is matched with the query
- [Return schema](link_to_return_schema?)




