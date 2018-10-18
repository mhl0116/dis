import os
import json
import traceback

import time
import commands

def get(cmd, returnStatus=False):
    status, out = commands.getstatusoutput(cmd)
    if returnStatus: return status, out
    else: return out

def cmd(cmd, returnStatus=False):
    status, out = commands.getstatusoutput(cmd)
    if returnStatus: return status, out
    else: return out

def proxy_hours_left():
    try:
        info = get("voms-proxy-info")
        hours = int(info.split("timeleft")[-1].strip().split(":")[1])
    except: hours = 0
    return hours

def proxy_renew():
    # http://www.t2.ucsd.edu/tastwiki/bin/view/CMS/LongLivedProxy
    cert_file = "/home/users/{0}/.globus/proxy_for_{0}.file".format(get("whoami").strip())
    if os.path.exists(cert_file): cmd("voms-proxy-init -q -voms cms -hours 120 -valid=120:0 -cert=%s" % cert_file)
    else: 
        cmd("voms-proxy-init -hours 9876543:0 -out=%s" % cert_file)
        cmd("cp /tmp/x509up_u{} .".format(os.getuid()))

def get_proxy_file():
    cert_file = os.path.abspath('x509up_u%s' % str(os.getuid()))
    return cert_file

def get_url_with_cert(url, return_raw=False):
    # need FULL PATH? -- yeah need full path
    cert_file = get_proxy_file()
    # url = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datasets?primary_ds_name=DY*&processed_ds_name=*100X*&data_tier_name=MINIAODSIM&detail=0"
    url = url.replace("[","\\[").replace("]","\\]")
    cmd = "curl -s --capath /etc/grid-security/certificates/ -E {0} --cacert {0} \"{1}\"".format(cert_file, url)
    stat,output = commands.getstatusoutput(cmd)
    # print output
    if return_raw: return output
    else: 
        # return ast.literal_eval(output)
        return json.loads(output)

def get_dbs_instance(dataset):
    if dataset.endswith("/USER"): return "phys03"
    elif "Nick" in dataset: return "phys03"
    else: return "global"

def dataset_event_count(dataset):
    # get event count and other information from dataset
    url = "https://cmsweb.cern.ch/dbs/prod/%s/DBSReader/filesummaries?dataset=%s&validFileOnly=1" % (get_dbs_instance(dataset),dataset)
    ret = get_url_with_cert(url)
    if type(ret) == dict and (ret.get("type",None)=="HTTPError") and "message" in ret:
        raise RuntimeError("Exception from DBS: {0}".format(ret["message"]))
    if len(ret) > 0:
        if ret[0]:
            return { "nevents": ret[0]['num_event'], "filesizeGB": round(ret[0]['file_size']/1.0e9,2), "nfiles": ret[0]['num_file'], "nlumis": ret[0]['num_lumi'] }
    return None

def list_of_datasets(wildcardeddataset, short=False, selectors=[]):
    if wildcardeddataset.count("/") != 3:
        raise RuntimeError("You need three / in your dataset query")

    _, pd, proc, tier = wildcardeddataset.split("/")
    # url = "https://cmsweb.cern.ch/dbs/prod/%s/DBSReader/datasets?dataset=%s&detail=0" % (get_dbs_instance(wildcardeddataset),wildcardeddataset)
    extra = ""
    if any(x in selectors for x in ["any","all","invalid","production"]):
        extra += "&dataset_access_type=*"
    url = "https://cmsweb.cern.ch/dbs/prod/%s/DBSReader/datasets?primary_ds_name=%s&processed_ds_name=%s&data_tier_name=%s&detail=0%s" % (get_dbs_instance(wildcardeddataset),pd,proc,tier,extra)
    ret = get_url_with_cert(url)
    if len(ret) > 0:
            vals = []

            if short: vals = [d["dataset"] for d in ret]
            else: 

                def get_info(d):
                    info = dataset_event_count(d["dataset"])
                    info["dataset"] = d["dataset"]
                    return info

                if len(ret) > 150:
                    raise RuntimeError("Getting detailed information for all these datasets (%i) will take too long" % len(ret))

                try:
                    from multiprocessing.dummy import Pool as ThreadPool 
                    pool = ThreadPool(8)
                    vals = pool.map(get_info, ret)
                    pool.close()
                    pool.join()
                except:
                    vals = map(get_info, ret)
                    

            return vals
    return []

def get_dataset_files(dataset, run_num=None,lumi_list=[],selectors=[]):
    # return list of 3-tuples (LFN, nevents, size_in_GB) of files in a given dataset
    valid = "1"
    if any(x in selectors for x in ["any","all","invalid","production"]):
        valid = "0"
    url = "https://cmsweb.cern.ch/dbs/prod/%s/DBSReader/files?dataset=%s&validFileOnly=%s&detail=1" % (get_dbs_instance(dataset),dataset,valid)
    if run_num and lumi_list:
        url += "&run_num=%i&lumi_list=[%s]" % (run_num, ",".join(map(str,lumi_list)))
    ret = get_url_with_cert(url)
    files = []
    for f in ret:
        files.append( [f['logical_file_name'], f['event_count'], f['file_size']/1.0e9] )
    return files

def get_dataset_runs(dataset):
    url = "https://cmsweb.cern.ch/dbs/prod/%s/DBSReader/runs?dataset=%s" % (get_dbs_instance(dataset),dataset)
    return get_url_with_cert(url)[0]["run_num"]
    
def get_pick_events(dataset, runlumievts):
    d_runs = {}
    for rle in runlumievts:
        if not rle: continue
        run,lumi,evt = map(int,rle.split(":"))
        if run not in d_runs: d_runs[run] = []
        d_runs[run].append([lumi,evt])

    for_map = [{"dataset":dataset, "run":run, "lumis":[lumi for lumi,evt in d_runs[run]]} for run in d_runs.keys()]

    def get_info(info):
        filesinfo = get_dataset_files(info["dataset"],run_num=info["run"],lumi_list=info["lumis"])
        return {"filesinfo": filesinfo, "run": info["run"], "fail": len(filesinfo)==0}

    try:
        from multiprocessing.dummy import Pool as ThreadPool 
        pool = ThreadPool(8)
        vals = pool.map(get_info, for_map)
        pool.close()
        pool.join()
    except:
        vals = map(get_info, for_map)

    files, runs_failed_to_find = [], []
    for d_info in vals:
        if d_info["fail"]: runs_failed_to_find.append(d_info["run"])
        else: files.extend([f[0] for f in d_info["filesinfo"]])

    files = list(set(files))
    payload = {
            "files": files,
            "cmd": "events='{0}'; files='{1}'; edmCopyPickMerge outputFile=output.root eventsToProcess=${{events}} inputFiles=${{files}}".format(
                ",".join([":".join(rle.split(":")[::2]) for rle in runlumievts]),
                ",".join(files),
                ),
            }
    warning = ("Failed to find run(s) %s." % (",".join(map(str,runs_failed_to_find)))) if runs_failed_to_find else ""

    return payload, warning

def get_dataset_config(dataset):
    import datetime
    url = "https://cmsweb.cern.ch/dbs/prod/%s/DBSReader/outputconfigs?dataset=%s" % (get_dbs_instance(dataset),dataset)
    ret = get_url_with_cert(url)
    info = ret[0]
    if "creation_date" in info:
        info["creation_date"] = str(datetime.datetime.fromtimestamp(info["creation_date"]))
    return ret[0]

def get_dataset_parent(dataset):
    # get parent of a given dataset
    ret = get_url_with_cert("https://cmsweb.cern.ch/dbs/prod/%s/DBSReader/datasetparents?dataset=%s" % (get_dbs_instance(dataset),dataset))
    if len(ret) < 1: return None
    return ret[0].get('parent_dataset', None)

def get_specified_parent(dataset, typ="LHE", fallback=None):
    # recurses up the tree of parent datasets until it finds the gen_sim dataset
    found = False
    fallback_found = False

    the_dataset = None
    fallback_dataset = None
    for i in range(4):
        dataset = get_dataset_parent(dataset)
        if not dataset: break
        if typ in dataset:
            found = True
            the_dataset = dataset
            break
        elif fallback and fallback in dataset: 
            fallback_found = True
            fallback_dataset = dataset

    if found:
        return the_dataset
    elif fallback_found:
        return fallback_dataset
    else:
        raise LookupError("Could not find parent dataset")

def get_file_replicas(filename):
    payload = get_url_with_cert("https://cmsweb.cern.ch/phedex/datasvc/json/prod/fileReplicas?lfn=%s" % filename)
    block = payload["phedex"]["block"][0]
    return {"block": block}

def get_replica_fractions(datasets):

    vals = []

    def get_info(dataset):
        payload = get_url_with_cert("https://cmsweb.cern.ch/phedex/datasvc/json/prod/blockReplicas?dataset=%s" % dataset)
        blocks = payload["phedex"]["block"]
        d_sites = {}
        ngbtot = 0.01
        for block in blocks:
            ngbtot += block["bytes"]/(1.e9)
            for replica in block["replica"]:
                if not replica["complete"]: continue
                site = replica["node"]
                ngb = replica["bytes"]/(1.e9)
                if site not in d_sites: d_sites[site] = 0.01
                d_sites[site] += ngb
        sitepairs = [[s,round(d_sites[s]/ngbtot,3)] for s in d_sites]
        return {"dataset": dataset, "site_fractions": sorted(sitepairs,key=lambda k: k[1], reverse=True)}


    if len(datasets) > 150:
        raise RuntimeError("Getting detailed information for all these datasets (%i) will take too long" % len(datasets))

    try:
        from multiprocessing.dummy import Pool as ThreadPool 
        pool = ThreadPool(8)
        vals = pool.map(get_info, datasets)
        pool.close()
        pool.join()
    except:
        vals = map(get_info, datasets)

    return vals

def get_mcm_json(dataset):
    # get McM json for given dataset
    # don't use first / in dataset name or else things break now  (As of ~Nov 2017, stupid MCM)
    url = "https://cms-pdmv.cern.ch/mcm/public/restapi/requests/produces/"+dataset[1:]
    mcm_json = json.loads(get_url_with_cert(url, return_raw=True))
    return mcm_json

def get_mcm_setup(campaign):
    # get McM json for given dataset
    url = "https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_setup/"+campaign
    # ret_data = json.loads(get_url_with_cert(url, return_raw=True))
    ret_data = get_url_with_cert(url, return_raw=True)
    try:
        ret_data = json.loads(ret_data)
    except: pass
    if "#!/bin/bash" in ret_data: ret_data = "\n" + ret_data
    return ret_data

def get_slim_mcm_json(dataset):
    out = {}
    mcm_json = get_mcm_json(dataset)
    
    try: out['cross_section'] = mcm_json['results']['generator_parameters'][-1]['cross_section']
    except: pass

    try: out['filter_efficiency'] = mcm_json['results']['generator_parameters'][-1]['filter_efficiency']
    except: pass

    try: out['match_efficiency'] = mcm_json['results']['generator_parameters'][-1]['match_efficiency']
    except: pass

    try: out['cmssw_release'] = mcm_json['results']['cmssw_release']
    except: pass

    try: out['mcdb_id'] = mcm_json['results']['mcdb_id']
    except: pass

    try: out['status'] = mcm_json['results']['status']
    except: pass

    try: out['fragment'] = mcm_json['results']['fragment']
    except: pass

    return out

def filelist_to_dict(files, short=False, num=10):
    newfiles = []
    for f in files:
        newfiles.append({"name": f[0], "nevents": f[1], "sizeGB": round(f[2],2)})
    if short: newfiles = newfiles[:num]
    return newfiles

def make_response(query, payload, failed, fail_reason, warning):


    status = "success"
    if failed: status = "failed"

    timestamp = int(time.time())
    d =  { "query": query, "timestamp": timestamp, "response": { "status": status, "fail_reason": fail_reason, "warning": warning, "payload": payload } } 

    cmd("echo {0} {1} {2} {3} >> log.txt".format(timestamp, query["type"], query.get("short",False), failed))

    # print d["response"]["fail_reason"]
    return json.dumps(d)
    # return { "query": query, "response": { "status": status, "fail_reason": fail_reason, "payload": payload } }

def get_derived_quantity(payload, key):
    if key == "eff_lumi":
        nevents = payload.get("nevents_out",-1)
        xsec = payload.get("xsec",-1)
        kfact = payload.get("kfact",-1)
        efact = payload.get("efact",-1)
        return round(nevents/(xsec*kfact*efact*1000.),5)
    return -1

def get_snt_samples(entity, selectors, short=True):

    from db import DBInterface
    db = DBInterface(fname="allsamples.db")

    match_dict = {"dataset_name": entity}

    # if we didn't specify a sample_type, then assume we only want CMS3 and not BABY
    if not selectors or not any(map(lambda x: "sample_type" in x, selectors)):

        # but if user specified an analysis, then don't restrict to CMS3
        if not any(map(lambda x: "analysis" in x, selectors)) \
                and not any(map(lambda x: "baby_tag" in x, selectors)):
            selectors.append("sample_type=CMS3")

    if selectors:
        for more in selectors:
            key = more.split("=")[0].strip()
            val = more.split("=")[1].strip()
            match_dict[key] = val


    samples = db.fetch_samples_matching(match_dict)
    samples = sorted(samples, key = lambda x: x.get("cms3tag","")+str(x.get("nevents_out","0")), reverse=True)

    if short:
        # import datetime
        new_samples = []
        for sample in samples:
            if sample["sample_type"] == "CMS3":
                for key in ["sample_id","filter_type","assigned_to", \
                            "comments","twiki_name", "analysis", "baby_tag"]:
                    del sample[key]
            elif sample["sample_type"] == "BABY":
                for key in ["sample_id","filter_type","assigned_to", \
                            "comments","twiki_name","xsec","gtag","nevents_in","nevents_out","kfactor","filter_eff"]:
                    del sample[key]
            # if "timestamp" in sample:
            #     sample["timestamp"] = str(datetime.datetime.fromtimestamp(sample["timestamp"]))
            new_samples.append(sample)
        samples = new_samples

    return samples

def get_pick_cms4(entity, selectors):
    samples = get_snt_samples(entity, [])
    sample = samples[0]
    metadata = "{0}/metadata.json".format(sample["location"])
    dout, warning = get_pick_events(entity, selectors)
    storefiles = dout["files"]
    ijobs = []
    with open(metadata,"r") as fhin:
        data = json.load(fhin)
        ijob_to_miniaod = data["ijob_to_miniaod"]
        ijobs = set([i for i,v in ijob_to_miniaod.items() if any(s in v for s in storefiles)])
    fnames = ["{0}/merged_ntuple_{1}.root".format(sample["location"],i) for i in ijobs]
    return fnames

def handle_query(arg_dict):

    if not arg_dict: return


    query_type = arg_dict.get("type","basic")
    query = arg_dict.get("query", None).strip()
    short = arg_dict.get("short", False)


    # parse extra information in query if it's not just the dataset
    # /Gjet*/*/*, cms3tag=*07*06* | grep location,dataset_name
    # ^^dataset^^ ^^^selectors^^^   ^^^^^^^^^^^pipes^^^^^^^^^^
    selectors = []
    pipes = []
    if "|" in query:
        first = query.split("|")[0].strip()
        pipes = query.split("|")[1:]

        if "," in first:
            entity = first.split(",")[0].strip()
            selectors = first.split(",")[1:]
        else:
            entity = first
    elif "," in query:
        entity = query.split(",")[0].strip()
        selectors = query.split(",")[1:]
    else:
        entity = query.strip()

    failed = False
    fail_reason = ""
    warning = ""
    payload = {}

    if "*" in entity and query_type in ["basic","files"]:
        query_type = "listdatasets"

    # # HUGE SLOWDONW (4 seconds :( )
    # if query_type in ["basic", "files", "listdatasets", "mcm", "driver", "lhe", "parents", "dbs"]:
    #     if proxy_hours_left() < 5: proxy_renew()

    if not entity:
        failed = True
        fail_reason = "Dataset not specified"


    try:
        if query_type == "snt":
            payload = get_snt_samples(entity, selectors, short)

        elif query_type == "basic":
            info = dataset_event_count(entity)
            if not info:
                failed = True
                fail_reason = "Dataset not found"
            payload = info

        elif query_type == "listdatasets":
            datasets = list_of_datasets(entity, short, selectors=selectors)
            if not datasets:
                failed = True
                fail_reason = "No datasets found"
            payload = datasets

        elif query_type == "sites":
            if "/store/" in entity:
                payload = get_file_replicas(entity)
            else:
                datasets = list_of_datasets(entity, short=True)
                if not datasets:
                    failed = True
                    fail_reason = "No datasets found"
                payload = get_replica_fractions(datasets)

        elif query_type == "files":
            files = get_dataset_files(entity, selectors=selectors)
            payload = filelist_to_dict(files, short, num=10)

        elif query_type == "runs":
            payload = get_dataset_runs(entity)

        elif query_type == "pick":
            payload, warning = get_pick_events(entity, selectors)

        elif query_type == "pick_cms4":
            payload = get_pick_cms4(entity, selectors)

        elif query_type == "config":
            config_info = get_dataset_config(entity)
            payload = config_info

        elif query_type == "mcm":
            if selectors:
                if "this" == selectors[0].lower():
                    gen_sim = entity
            else:
                gen_sim = get_specified_parent(entity, typ="GEN-SIM", fallback="AODSIM")

            if short:
                info = get_slim_mcm_json(gen_sim)
            else:
                info = get_mcm_json(gen_sim)["results"]
            info["sample"] = gen_sim
            payload = info

        elif query_type == "parents":

            lineage = []
            dataset = entity
            while dataset:
                try:
                    dataset = get_dataset_parent(dataset)
                except: 
                    break
                if dataset: lineage.append(dataset)

            info = {}
            info["parents"] = lineage
            payload = info

        elif query_type == "driver":
            if selectors:
                if "this" == selectors[0].lower():
                    gen_sim = entity
            else:
                gen_sim = get_specified_parent(entity, typ="GEN-SIM", fallback="AODSIM")

            info = get_mcm_json(gen_sim)["results"]
            dataset_base = info["dataset_name"]
            campaign = info["prepid"]
            driver = get_mcm_setup(campaign)
            payload = { "dataset": dataset_base, "cmsDriver": driver }

        elif query_type == "lhe":
            lhe = get_specified_parent(entity, typ="LHE")
            # lhe = get_dataset_parent(gen_sim)
            files = get_dataset_files(lhe)
            payload["files"] = filelist_to_dict(files, short)


        elif query_type == "update_snt":
            from db import DBInterface
            db = DBInterface(fname="allsamples.db")
            s = {}
            for keyval in query.split(","):
                key,val = keyval.strip().split("=")
                s[key] = val
            payload = s
            did_update = db.update_sample(s)
            db.close()

            payload["updated"] = did_update

        elif query_type == "delete_snt":
            from db import DBInterface
            db = DBInterface(fname="allsamples.db")
            s = {}
            for keyval in query.split(","):
                key,val = keyval.strip().split("=")
                s[key] = val
            payload = s
            did_delete = db.delete_sample(s)
            db.close()

            payload["deleted"] = did_delete

        elif query_type == "dbs":
            if "raw" in selectors:
                import base64
                payload = get_url_with_cert(query.rsplit(",",1)[0],return_raw=True)
                payload = base64.b64encode(payload)
            else:
                payload = get_url_with_cert(query)
            # payload = "/DYJetsToLL_M-50_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/MINIAODSIM"
            # payload = "/DYJetsToLL_M-50_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/MINIAODSIM"

        else:
            failed = True
            fail_reason = "Query type not found"
    except:
        failed = True
        fail_reason = traceback.format_exc()

    if pipes:
        for pipe in pipes:
            pipe = pipe.strip()
            parts = pipe.split(" ",1)
            if len(parts) == 1:
                verb = parts[0].strip()
            elif len(parts) == 2:
                verb, keys = parts
                keys = map(lambda x: x.strip(), keys.split(","))
            else:
                verb = parts[0]

            if verb == "grep":
                if type(payload) == list:
                    for ipay in range(len(payload)):
                        if type(payload[ipay]) is not dict: continue

                        if len(keys) > 1:
                            d = {}
                            for key in keys:
                                if key in payload[ipay]:
                                    d[key] = payload[ipay][key]
                                else:
                                    d[key] = get_derived_quantity(payload[ipay], key)
                            payload[ipay] = d
                        else:
                            payload[ipay] = payload[ipay].get(keys[0],None)
                elif type(payload) == dict:
                    if len(keys) > 0:
                        d = {}
                        for key in keys: d[key] = payload.get(key,None)
                        payload = d
            elif verb == "stats":
                if type(payload) == list:
                    nums = []
                    for elem in payload:
                        try: nums.append(float(elem))
                        except: pass

                    if len(nums) > 0:
                        payload = {
                                "N": len(nums),
                                "total": sum(nums),
                                "minimum": min(nums),
                                "maximum": max(nums),
                                }
                    else:
                        payload = {"N": len(payload)}
            elif verb == "sort":
                if type(payload) == list:
                    payload = sorted(payload)

    return make_response(arg_dict, payload, failed, fail_reason, warning)


if __name__=='__main__':

    arg_dict = {}

    # arg_dict = {"query": "/TChiNeu*/namin-TChiNeu*/USER", "type":"files"}
    # arg_dict = {"type": "snt", "query": "/G* | grep cms3tag"}
    # arg_dict = {"type": "driver", "query": "/QCD_HT2000toInf_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIIFall15MiniAODv2-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1/MINIAODSIM"}
    # arg_dict = {"type": "lhe", "query": "/SMS-T5qqqqWW_mGl-600to800_mLSP-0to725_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15MiniAODv2-FastAsympt25ns_74X_mcRun2_asymptotic_v2-v1/MINIAODSIM", "short":"short"}
    # arg_dict = {"type": "driver", "query": "/SMS-T5qqqqWW_mGl-600to800_mLSP-0to725_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring15MiniAODv2-FastAsympt25ns_74X_mcRun2_asymptotic_v2-v1/MINIAODSIM"}
    # arg_dict = {"type": "parents", "query": "/SMS-T1tttt_mGluino-1500_mLSP-100_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/MINIAODSIM"}
    # arg_dict = {"type": "mcm", "query": "/QCD_Pt-80to120_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8/RunIIFall15MiniAODv2-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1/MINIAODSIM | grep cross_section", "short":"short"}
    # arg_dict = {"type": "mcm", "query": "/QCD_Pt-80to120_MuEnrichedPt5_TuneCUETP8M1_13TeV_pythia8/RunIIFall15MiniAODv2-PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1/MINIAODSIM", "short":"short"}
    # arg_dict = {"type": "update_snt", "query": "dataset_name=test,cms3tag=CMS3_V07-06-03_MC,sample_type=CMS3,gtag=test,location=/hadoop/crap/crap/", "short":"short"}
    # arg_dict = {"type": "basic", "query": "/SingleElectron/Run2016B-PromptReco-v1/MINIAOD"}
    # arg_dict = {"type": "dbs", "query": "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/files?dataset=/DYJetsToLL_M-50_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISpring16MiniAODv1-PUSpring16_80X_mcRun2_asymptotic_2016_v3-v1/MINIAODSIM&detail=1&lumi_list=[134007]&run_num=1"}
    # arg_dict = {"type": "runs", "query": "/SinglePhoton/Run2016E-PromptReco-v2/MINIAOD"}
    # arg_dict = {"type": "pick", "query": "/MET/Run2016D-PromptReco-v2/MINIAOD,276524:9999:2340928340,276525:2892:550862893,276525:2893:823485588,276318:300:234982340,276318:200:234982340"}
    # arg_dict = {"type": "snt", "query": "/WJetsToLNu_TuneCUETP8M1_13TeV-madgr*"}
    # arg_dict = {"type": "pick_cms4", "query": "/MET/Run2016D-03Feb2017-v1/MINIAOD,276525:2892:550862893,276525:2893:823485588,276318:300:234982340,276318:200:234982340"}
    # arg_dict = {"type": "snt", "query": "/SingleElectron/Run2016*-PromptReco-v*/MINIAOD | grep nevents_in | stats", "short":"short"}
    # arg_dict = {"type": "driver", "query": "/tHq_HToTT_0J_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-PUSummer16Fast_80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM"}
    # arg_dict = {"type": "basic", "query": "/*/Run2016*-17Jul2018-v1/MINIAOD", "short":"short"}
    # arg_dict = {"type": "basic", "query": "/DY*/*18MiniAOD*/MINIAODSIM", "short":"short"}
    # arg_dict = {"type": "sites", "query": "/ZeroBias/Run2016F-17Jul2018-v1/MINIAOD", "short":"short"}
    # arg_dict = {"type": "snt", "query": "/DY*/*MiniAOD*/MINIAODSIM | grep nevents_out | sort", "short":"short"}
    # arg_dict = {"type": "basic", "query": "/DoubleEG/Run2017F-09May2018-v1/d", "short":"short"}
    
    print handle_query(arg_dict)

#     import pprint
#     pprint.pprint(json.loads(handle_query(arg_dict)))
#     print
#     print json.loads(handle_query(arg_dict))["response"]["fail_reason"]
