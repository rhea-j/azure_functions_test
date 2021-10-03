import logging

import azure.functions as func
from osgeo import gdal


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    fname = req.params.get('file')
    if not fname:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            fname = req_body.get('file')
    fname = fname.replace('http://', 'https://')
    fname = "/vsicurl/" + fname
    # open and return metadata
    ds = gdal.Open(fname)
    band = ds.GetRasterBand(1)
    stats = band.GetStatistics(0, 1)

    return func.HttpResponse(
        str(stats),
        status_code=200
    )
