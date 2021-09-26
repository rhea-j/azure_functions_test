import logging
import pandas as pd
import requests
import gzip
import urllib.request
from io import BytesIO 

import azure.functions as func


import memory_profiler
root_logger = logging.getLogger()
root_logger.handlers[0].setFormatter(logging.Formatter("%(name)s: %(message)s"))
profiler_logstream = memory_profiler.LogFile('memory_profiler_logs', True)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    start = req.params.get('start')
    end = req.params.get('end')
    if not start:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            start = req_body.get('start')
    if not end:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            end = req_body.get('end')
    resp = get_mtl(start,end)

    return func.HttpResponse(str(resp))

@memory_profiler.profile(stream=profiler_logstream)
def get_mtl(start, end):
    start = int(start)
    end = int(end)
    if start is not None and end is not None: 
        URL = 'https://landsat-pds.s3.amazonaws.com/c1/L8/scene_list.gz'
        inmemory = BytesIO(urllib.request.urlopen(URL).read())
        fStream = gzip.GzipFile(fileobj=inmemory, mode='rb')
        count=  0
        data_list = []
        data_dict = {}
        start = start + 1
        while True:
            line = fStream.readline().rstrip()
            if count >=start:
                if count > end:
                    break
                str_path = line.decode("utf-8")
                index_path = str_path.split(',')[11]         
                scene_path = index_path.split("/")[8]
                scene_path += "_MTL.txt"
                mtl_path = index_path.replace('index.html',scene_path)
                count = count +1
                r = requests.get(url = mtl_path)
                obj_data = r.text
                obj_key = mtl_path
                data_dict= {
                    "path": obj_key,
                    "tag": 'MS',
                    "displayName": obj_key,
                    "groupName":obj_key,
                    "productName": 'L1GT',
                    "uriProperties": {"textBuffer":obj_data}
                }
                data_list.append(data_dict)  
                
            elif count<start:
                count = count +1
                continue   
    resp = {"Keys":int(end)-int(start)+1,"Data":data_list}   
    return resp    
