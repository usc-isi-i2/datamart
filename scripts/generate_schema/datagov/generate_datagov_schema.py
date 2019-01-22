import jsonlines
import json
import urllib
import ssl
import os
import urllib.request
import sys 


args1  = sys.argv[1]
args2  = sys.argv[2]

counterror = 0
context = ssl._create_unverified_context()
for num in range(0,303):         
	curPath=os.getcwd()
	tempPath=str(num)
	targetPath=str(args2)+"/"+tempPath
	if not os.path.exists(targetPath):
		os.makedirs(targetPath)
	else:
		print('already have it')


	with jsonlines.open(str(args1)+'/output'+str(num)+'.jsonl') as reader:
	    for obj in reader:
	    	for element in obj["resources"]:
	    		if element["mimetype"] is None:
		    		pass
		    	else:
		    		urlline = element["url"]
		    		utf8string = element["mimetype"]#.encode("utf-8")  
		    		if('csv' in utf8string):
		    				# url
		    			
		    			urllineStr = urlline.replace("https","http")
		    			#print urllineStr
		    			headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36"}
		    			try:
		   					code = 0
		   					#print code
		   					#with open('filename','wb') as f:
		   						#print code
		   					req = urllib.request.Request(urllineStr, None, headers)
		   					#print code
		   					code = urllib.request.urlopen(req,context = context,timeout=8.0).getcode()#.read()
		    				#csv = urllib2.urlopen(req).read()
		    					#f.write(html)
		    					#f.close()
		    					#print code
		    			except Exception as e:
		    				print (num)
		    				print (urllineStr)
		    				print (e)
		   					#counterror = counterror + 1

		    			#print code
		    			if code == 200:
		    				#print (obj)
		    				#print("\n")
		    				data = dict()
		    				data["title"] = obj["title"]#.encode("utf-8")  # note name highlevel
		    				data["description"] = element["description"]##.encode("utf-8")
		    				#print (data)
		    				#print("\n")
		    				if obj["url"] is None:
		    					data["url"] = obj["url"]
		    				else:
		    					data["url"] = obj["url"]#.encode("utf-8") 
		    				#print (data)
		    				#print("\n")
		    				data["keywords"] = []
		   					#data["keywords"].append(obj["title"].encode("utf-8"))
		    				for tag in obj["tags"]:
		   						if tag["name"] is None:
		   							pass
		   						else:
		   							data["keywords"].append(tag["name"])
		    				data["date_published"]= element["created"]#.encode("utf-8")
		    				data["date_updated"]= None
		    				data["license"] = None#obj["license_url"]
		    				data["provenance"] = dict()
		    				data["provenance"]["source"] = "data.gov"
		    				data["original_identifier"] = element["id"]#.encode("utf-8")
		    				data["materialization"] = dict()
		    				data["materialization"]["python_path"]= "datagov_materializer"
							# "arguments" must be an object:
		    				data["materialization"]["arguments"] = {"url": urllineStr} #urllineStr#.encode("utf-8")
		    				data["variable"] = []
		    				data["additional_info"] = dict()
		    				data["additional_info"]["groups"]=[]
		    				for group in obj["groups"]:
		   						tmpGroup = dict()
		   						tmpGroup["display_name"] = group["display_name"]#.encode("utf-8")
		   						tmpGroup["description"]= group["description"]#.encode("utf-8")
		   						tmpGroup["title"] = group["title"]#.encode("utf-8")
		   						tmpGroup["id"] = group["id"]#.encode("utf-8")
		   						tmpGroup["name"] = group["name"]#.encode("utf-8")
		    				#print(data)
		    				#print("\n")

		    				data["additional_info"]["organization"] = dict()
		    				data["additional_info"]["organization"]["description"]=obj["organization"]["description"]#.encode("utf-8")
		    				data["additional_info"]["organization"]["created"]=obj["organization"]["created"]#.encode("utf-8")
		    				data["additional_info"]["organization"]["title"]=obj["organization"]["title"]#.encode("utf-8")
		    				data["additional_info"]["organization"]["name"]=obj["organization"]["name"]#.encode("utf-8")
		    				data["additional_info"]["organization"]["is_organization"]=obj["organization"]["is_organization"]
		    				data["additional_info"]["organization"]["state"]=obj["organization"]["state"]#.encode("utf-8")
		    				data["additional_info"]["organization"]["image_url"]=obj["organization"]["image_url"]#.encode("utf-8")
		    				data["additional_info"]["organization"]["revision_id"]=obj["organization"]["revision_id"]#.encode("utf-8")
		    				data["additional_info"]["organization"]["type"]=obj["organization"]["type"]#.encode("utf-8")
		    				data["additional_info"]["organization"]["id"]=obj["organization"]["id"]#.encode("utf-8")
		    				data["additional_info"]["organization"]["approval_status"]=obj["organization"]["approval_status"]#.encode("utf-8")

		    				data["additional_info"]["extras"]=[]
		    				for extra in obj["extras"]:
		    					tmpExtra = dict()
		    					tmpExtra["key"] = extra["key"]#.encode("utf-8")
		    					#if isinstance(extra["value"], unicode):
		    					tmpExtra["value"]=extra["value"]#.encode("utf-8")
		    					#else:
		    						#tmpExtra["value"]=extra["value"]
		    					data["additional_info"]["extras"].append(tmpExtra)


		    				fileName='/'+data["original_identifier"]+'.json'
		    				filePath=targetPath+fileName

		    				f = open(filePath, 'w+')
		    				f.write(json.dumps(data))
		    				#print("sss")
		    				f.close








