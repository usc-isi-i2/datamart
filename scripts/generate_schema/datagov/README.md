# datagov README

1. crawl.py is the python script to crawl metadata from data.gov

   ​	**python3 crawl.py \<the folder you want to store the files>**

2. generate_datagov_schema.py is the python script to convert the old schema to the new schema we need

   ​	**python3 generate_datagov_schema.py \<the folder you store the original files> \<the folder you store the generate files >**

   **example**:   python3 generate_datagov_schema.py /Users/apple/data_gov /Users/apple/Desktop/data_gov/output

   ​

   **tip**: should install jsonlines first: **pip install jsonlines**