
import os
import sys
import time
import traceback

import requests
import requests_cache

from flask import Flask
from flask import request, jsonify

app = Flask(__name__)

from snt_db import SNTDBInterface
from api import Fetcher, \
        DBSApi, PhedexApi, MCMApi, \
        SNTApi, PMPApi, XSDBApi, ReqMgrApi
from utils import transform_output, transform_input, enable_requests_caching

enable_requests_caching("maincache",expire_after=3000)
f = Fetcher()
sntdb = SNTDBInterface(fname="allsamples.db")
def do_query(query, query_type, short=True):

    entity, selectors, pipes = transform_input(query)

    ret = {}
    if query_type == "snt": ret = SNTApi(db=sntdb).get_samples(entity, selectors, short)

    elif query_type == "basic":
        if "*" in entity: ret = DBSApi(fetcher=f).get_list_of_datasets(entity, short=short, selectors=selectors)
        else: ret = DBSApi(fetcher=f).get_dataset_event_count(entity)
    elif query_type == "files": ret = DBSApi(fetcher=f).get_dataset_files(entity, selectors=selectors, max_files=(10 if short else None), to_dict=True)
    elif query_type == "runs": ret = DBSApi(fetcher=f).get_dataset_runs(entity)
    elif query_type == "config": ret = DBSApi(fetcher=f).get_dataset_config(entity)
    elif query_type == "mcm": ret = MCMApi(fetcher=f).get_driver_chain_from_dataset(entity, first_only=True)
    elif query_type == "parents": ret = DBSApi(fetcher=f).get_dataset_parents(entity)
    elif query_type == "chain": ret = MCMApi(fetcher=f).get_driver_chain_from_dataset(entity, first_only=False)
    elif query_type == "update_snt": ret = SNTApi(db=sntdb).update_sample(query)
    elif query_type == "delete_snt": ret = SNTApi(db=sntdb).delete_sample(query)
    elif query_type == "dbs": ret = DBSApi(fetcher=f).get_arbitrary_url(entity)
    elif query_type == "xsdb": ret = XSDBApi(fetcher=f).get_samples(entity)
    elif query_type == "psets": ret = ReqMgrApi(fetcher=f).get_info(entity, short=short)
    elif query_type == "sites":
        if "/store/" in entity:
            ret = PhedexApi(fetcher=f).get_file_replicas(entity)
        else:
            if short:
                ret = PhedexApi(fetcher=f).get_dataset_replica_fractions(entity)
            else:
                ret = PhedexApi(fetcher=f).get_file_replicas(entity,typ="dataset")

    if pipes:
        ret["payload"] = transform_output(ret["payload"],pipes)

    return ret

@app.after_request
def add_headers(response):
    response.headers.add('Content-Type', 'application/json')
    # response.headers.add('Cache-Control', 'max-age=300')
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Methods', 'PUT, GET, POST, DELETE, OPTIONS')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Expose-Headers', 'Content-Type,Content-Length,Authorization,X-Pagination')
    return response

@app.route('/dis/serve', methods=["GET"])
def main():
    query = request.args.get("query")
    short = bool(request.args.get("short",False))
    query_type = request.args.get("type","basic")
    t0 = time.time()
    try:
        if not len(query.strip()):
            raise Exception("Empty query")
        js = do_query(query,query_type=query_type,short=short)
    except:
        js = dict(
                urls=[],
                request_times=[],
                status="failed",
                payload={
                    "failure_reason": traceback.format_exc(),
                    },
                )
    t1 = time.time()
    duration = t1-t0
    return jsonify(js)

@app.route('/dis/clearcache')
def clearcache():
    requests_cache.clear()
    return jsonify(dict(status="success"))

@app.route('/dis/deleteproxy')
def deleteproxy():
    f.cookies = None
    f.cookie_expirations = {}
    os.system("rm /home/users/namin/private/ssocookie.txt")
    return jsonify(dict(status="success"))

if __name__ == '__main__':
    # app.run(host="localhost", port=8887, threaded=True, debug=True)
    app.run(host="0.0.0.0", port=50010, threaded=True, debug=True)

