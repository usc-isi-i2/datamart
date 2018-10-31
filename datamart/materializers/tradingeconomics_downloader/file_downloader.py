import json
import datetime
import os
import hashlib
import itertools
from dateutil.relativedelta import relativedelta

import requests
import wget
from tld import get_tld

from datamart.materializers.tradingeconomics_downloader.config import config


class FileDownloader():

    def __init__(self, dst_path):
        self.dst_path = os.path.abspath(dst_path)

    # process metadata and download file, if scheduled
    def process(self, dataset_file, force, current_datetime):

        data = dataset_file["where_to_download"]
        try:
            os.makedirs(self.dst_path)
        except OSError:
            pass
        cwd = os.getcwd()
        os.chdir(self.dst_path)

        urls = self.generate_url_list(data, current_datetime)

        self.downloadHelper(urls, data, force, current_datetime)

        os.chdir(cwd)

    @staticmethod
    def generate_url_list(data, current_datetime=None):
        # url list
        urls = []

        # get replication object
        replication = data.get("replication", list())
        if len(replication) == 0:
            urls.append(data["template"])
        else:

            # get all values for url generation
            dictionary = dict()
            for key in data["replication"]:
                arr = list()
                if "values" in data["replication"][key]:
                    arr = data["replication"][key]["values"]
                elif "format" in data["replication"][key]:
                    dt_format = data["replication"][key]["format"]
                    current_time = current_datetime or datetime.datetime.now()
                    if "delta" in data["replication"][key]:
                        delta = data["replication"][key]["delta"]
                        if dt_format in ("%y", "%Y"):  # year
                            delta = relativedelta(years=delta)
                        elif dt_format in ("%m", "%B", "%b"):  # month
                            delta = relativedelta(months=delta)
                        elif dt_format in ("%d", "%j"):  # day
                            delta = relativedelta(days=delta)
                        else:
                            # the unit for all other cases are "a day"
                            delta = relativedelta(days=delta)
                        current_time += delta
                    arr.append(current_time.strftime(dt_format))
                dictionary[key] = arr

            # get cross product for all values
            keys, values = dictionary.keys(), dictionary.values()
            cross_product = [dict(zip(keys, items)) for items in itertools.product(*values)]

            # generate urls
            for item in cross_product:
                urls.append(data["template"].format(**item))

        return urls

    def downloadHelper(self, urls, data, force, current_datetime):
        frequency = data["frequency"]
        identifier = data["identifier"]
        method = data.get("method", 'get')
        file_type = data['file_type']
        param = data['post'] if method == 'post' else dict()
        if self.checkDateMatch(frequency, current_datetime) or force:
            for url in urls:
                status_code, timestamp, fname = self.downloadFile(url, param, method, identifier, file_type)
                if status_code == 200:
                    self.addMetaData(url, timestamp, fname, identifier)

    # check if the file is scheduled to be downloaded for the current date
    def checkDateMatch(self, file_date, current_datetime):
        if len(file_date) == 0 or file_date == "never":
            return False

        if file_date == "daily":
            return True


        current_day = datetime.datetime.now().day if not current_datetime else current_datetime.day
        if file_date.startswith('dayofmonth:'):
            # print map(int, file_date[len('dayofmonth:'):].split(','))
            # print current_day
            return current_day in map(int, file_date[len('dayofmonth:'):].split(','))

        return False

    # download file using Requests API
    def downloadFile(self, link, parameters, method, identifier, file_type):
        # get timestamp
        ts = datetime.datetime.now().isoformat()
        # use SHA-256 to generate hash for filename + identifier
        # hex_digest = hashlib.sha256(link.encode() + str(ts).encode()).hexdigest()
        # hex_digest = hashlib.sha256(link.encode()).hexdigest()
        local_filename = identifier + '.' + file_type

        if link.startswith('ftp://'):
            # print link, local_filename
            try:
                wget.download(link, out=local_filename, bar=None)
                return 200, ts, local_filename
            except Exception as e:
                print('ftp download error', e)
                return 500, ts, local_filename

        else:
            # GET request
            if method == "get":
                r = requests.get(url=link, params=parameters, stream=True)
            # POST request
            elif method == "post":
                r = requests.post(url=link, params=parameters, stream=True)

            # if status is not okay
            if r.status_code != 200:
                print('http download error', r.status_code)
                return r.status_code, ts, local_filename

            # if status is okay
            elif r.status_code == 200:
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024):
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)

                return r.status_code, ts, local_filename

    def addMetaData(self, url, timestamp, path, identifier):
        # metadata (CDR)
        dict = {
            "url": url,
            "timestamp_crawl": timestamp,
            "raw_content_path": os.path.join(os.getcwd(), str(path)),
            "tld": get_tld(url),
            "dataset_identifier": identifier
        }
        # open file is exists or create new if not
        with open(config['metadata_file_name'], 'a') as f:
            # append JSONLINE at the end of metadata.jl
            f.write(json.dumps(dict) + '\n')
