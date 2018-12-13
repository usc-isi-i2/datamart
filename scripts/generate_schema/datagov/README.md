# datagov README

1. crawl.py is the python script to crawl metadata from data.gov

   ​	**python3 crawl.py \<the folder you want to store the files>**

2. generate_datagov_schema.py is the python script to convert the old schema to the new schema we need

   ​	**python3 generate_datagov_schema.py \<the folder you store the original files> \<the folder you store the generate files >**

   **example**:   python3 generate_datagov_schema.py /Users/apple/data_gov /Users/apple/Desktop/data_gov/output

   ​

   **tips**: should install jsonlines first: **pip install jsonlines**







#### Thought about crawl.py

the website of data.gov provides API document : https://docs.ckan.org/en/latest/api/index.html

I followed instruction of the API document to create this python script. 

Inside the script, there is "http://catalog.data.gov/api/3/action/package_search?rows=1000&start=x".

This means showing 1000 dataset's metadata from index x.

There are around 303945 datasets, so I designed a loop to download all the metadata as JSON Lines format.

this is the example to create a JSON Lines file: http://jsonlines.org/examples/

args1 is the folder where you want to store the output files.



#### Thought about generate_datagov_schema.py

args1 is the folder where you store the input files

args2 is the folder where you store the output files

First I designed a loop to iterate each JSON Lines files, and test each one's url  is valid or not.

If the url is valid, then I started to create this metadata in the schema we want by python's dict and  json.dumps.



**tips**: I replace "https" with "http" for easy web crawling. 



schema: https://paper.dropbox.com/doc/Datamart-Index-Schema--ATfjK4IoGNbeYHERQwweC4lbAg-0Uu03rDIUCttwS0x9GLCq











​																	create by Chen Lou 12/10/2018



