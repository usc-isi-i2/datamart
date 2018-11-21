import urllib.request as urlRequest
import urllib.parse as urlParse
import json
import sys


args = sys.argv[1]
url = "http://catalog.data.gov/api/3/action/package_search?rows=1000&start="
# pretend to be a chrome 47 browser on a windows 10 machine
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36"}

for num in range(0,303):

	req = urlRequest.Request(url+str(num*1000), headers = headers)
	# open the url
	x = urlRequest.urlopen(req)

	response_dict = json.loads(x.read())
	with open(str(args)+'/output'+str(num)+'.jsonl', 'w') as outfile:
	    for entry in response_dict['result']['results']:
	    	for entry2 in entry['resources']:
	    		if entry2['mimetype']!= None:
	    			if "csv" in entry2['mimetype']:
	    				json.dump(entry, outfile)
	    				outfile.write('\n')
	    				continue


#..........303945



