from flask import Flask
from flask import request, jsonify

import time

import requests
import requests_cache

app = Flask(__name__)

from snt_db import SNTDBInterface
from api import Fetcher, \
        DBSApi, PhedexApi, MCMApi, \
        SNTApi, PMPApi, XSDBApi
from utils import transform_output, transform_input, enable_requests_caching

enable_requests_caching("test3",expire_after=3000)
f = Fetcher()
sntdb = SNTDBInterface(fname="allsamples.db")
def do_query(query, query_type, short=True):

    entity, selectors, pipes = transform_input(query)

    if query_type == "snt": ret = SNTApi(db=sntdb).get_samples(entity, selectors, short)

    elif query_type == "basic":
        if "*" in entity: ret = DBSApi(fetcher=f).get_list_of_datasets(entity, short=short)
        else: ret = DBSApi(fetcher=f).get_dataset_event_count(entity)
    elif query_type == "files": ret = DBSApi(fetcher=f).get_dataset_files(entity, selectors=selectors, max_files=(10 if short else None), to_dict=True)
    elif query_type == "runs": ret = DBSApi(fetcher=f).get_dataset_runs(entity)
    elif query_type == "config": ret = DBSApi(fetcher=f).get_dataset_config(entity)
    elif query_type == "mcm": ret = MCMApi(fetcher=f).get_driver_chain_from_dataset(entity, first_only=True)
    elif query_type == "parents": ret = DBSApi(fetcher=f).get_dataset_parents(entity)
    elif query_type == "chain": ret = MCMApi(fetcher=f).get_driver_chain_from_dataset(entity, first_only=False)
    elif query_type == "update_snt": ret = SNTApi(db=sntdb).update_sample(entity,selectors)
    elif query_type == "delete_snt": ret = SNTApi(db=sntdb).delete_sample(entity,selectors)
    elif query_type == "dbs": ret = DBSApi(fetcher=f).get_arbitrary_url(entity)
    elif query_type == "xsdb": ret = XSDBApi(fetcher=f).get_samples(entity)
    elif query_type == "sites":
        if "/store/" in entity:
            ret = PhedexApi(fetcher=f).get_file_replicas(entity)
        else:
            if short:
                if "*" in entity:
                    datasets = DBSApi(fetcher=f).get_list_of_datasets(entity, short=True)["payload"]
                    ret = PhedexApi(fetcher=f).get_dataset_replica_fractions(datasets)
                else:
                    ret = PhedexApi(fetcher=f).get_dataset_replica_fractions(entity)
            else:
                ret = PhedexApi(fetcher=f).get_file_replicas(entity,typ="dataset")

    if pipes:
        ret["payload"] = transform_output(ret["payload"],pipes)

    return ret

@app.route('/main', methods=["GET"])
def main():
    print "---> Running main"
    query = request.args.get("query")
    short = request.args.get("short",True)
    query_type = request.args.get("query_type","basic")
    print request.args
    t0 = time.time()
    js = do_query(query,query_type=query_type,short=short)
    t1 = time.time()
    duration = t1-t0
    print duration
    return jsonify(js)

@app.route('/clearcache')
def clearcache():
    requests_cache.clear()
    return jsonify(dict(status="success"))

if __name__ == '__main__':
    app.run(host="localhost", port=8887, threaded=True, debug=True)

