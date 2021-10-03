import logging

import azure.functions as func
from osgeo import gdal
from cryptography.fernet import Fernet
import tempfile
import os
import requests
import pathlib
import json


def setConfig(connectionString):
    accountKey = connectionString['accountKey']
    accountName = connectionString['accountName']
    gdal.SetConfigOption('AZURE_STORAGE_ACCOUNT', accountName)
    gdal.SetConfigOption('AZURE_STORAGE_ACCESS_KEY', accountKey)
    gdal.SetConfigOption("AWS_HTTPS", "NO")
    gdal.SetConfigOption("OSS_HTTPS", "NO")
    gdal.SetConfigOption("CPL_AZURE_USE_HTTPS", "NO")
    gdal.SetConfigOption("GDAL_HTTP_USE_CAPI_STORE", "YES")
    gdal.SetConfigOption("CPL_DEBUG", "ON")

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    inputCloudStorePath = req.params.get('inputCloudStorePath')
    if not inputCloudStorePath:
         try:
             req_body = req.get_json()
         except ValueError:
             pass
         else:
             inputCloudStorePath = req_body.get('inputCloudStorePath')

    outputCloudStorePath = req.params.get('outputCloudStorePath')
    if not outputCloudStorePath:
         try:
             req_body = req.get_json()
         except ValueError:
             pass
         else:
             outputCloudStorePath = req_body.get('outputCloudStorePath')

    outputName = req.params.get('outputName')
    if not outputName:
         try:
             req_body = req.get_json()
         except ValueError:
             pass
         else:
             outputName = req_body.get('outputName')

    inputStorageConnectionString = req.params.get('inputStorageConnectionString')
    if not inputStorageConnectionString:
         try:
             req_body = req.get_json()
         except ValueError:
             pass
         else:
             inputStorageConnectionString = req_body.get('inputStorageConnectionString')

    outputStorageConnectionString = req.params.get('outputStorageConnectionString')
    if not outputStorageConnectionString:
         try:
             req_body = req.get_json()
         except ValueError:
             pass
         else:
             outputStorageConnectionString = req_body.get('outputStorageConnectionString')

    cipher_suite = Fernet(b'hoRlFLLC6XdCGAhSLZi4O2SYxqWbvW8sXxpk51OXCEE=')
    logging.info(inputStorageConnectionString)
    inputStorageConnectionString_decrypted = (cipher_suite.decrypt(inputStorageConnectionString.encode()))
    inputConnectionString = json.loads(inputStorageConnectionString_decrypted.decode("utf-8"))

    setConfig(inputConnectionString)
    gdal.AllRegister()


    ds = gdal.Open(inputCloudStorePath)

    if not outputName.endswith(".cog"):
        outputName  = outputName + ".cog"
        
    tempFilePath = os.path.join(tempfile.gettempdir(), outputName)
    op = gdal.Translate(tempFilePath, ds, format = "COG")


    #outputConnectionString, outputCloudPath = getCloudDetails(raurl, token, outputCloudStorePath)
    cipher_suite = Fernet(b'hoRlFLLC6XdCGAhSLZi4O2SYxqWbvW8sXxpk51OXCEE=')
    outputStorageConnectionString_decrypted = (cipher_suite.decrypt(outputStorageConnectionString.encode()))
    outputConnectionString = json.loads(outputStorageConnectionString_decrypted.decode("utf-8"))
    setConfig(outputConnectionString)
    gdal.AllRegister()
    logging.info(tempFilePath)
    logging.info(outputCloudStorePath)
    transfer = gdal.Sync(str(tempFilePath), str(outputCloudStorePath))

    return func.HttpResponse(
        json.dumps({"success":transfer}),
        status_code=200
    )
