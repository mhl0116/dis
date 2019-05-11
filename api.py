import requests
import os
import cookielib
import datetime
import time
from pprint import pprint

from config import API_URLS, AUTH_PATHS
from utils import transform_input, transform_output
from snt_db import SNTDBInterface

# import requests_cache

class Fetcher(object):
    def __init__(self):
        self.capath = AUTH_PATHS["capath"]
        self.cacert = AUTH_PATHS["cacert"]
        self.usercert = AUTH_PATHS["usercert"]
        self.userkey_passwordless = AUTH_PATHS["userkey_passwordless"]
        self.cookie_file = AUTH_PATHS["cookie_file"]

        self.session = requests.Session()
        # self.session = requests_cache.CachedSession()
        self.session.cert = self.cacert
        self.session.verify = self.capath

        # map url -> datetime object for cookie expirations
        self.cookie_expirations = {}
        self.cookies = None

    def get_sso_cookies(self,url,force_create=False):
        # if cookie expiration doesn't exist, or beyond cookie expiration, then need to update
        need_to_update = (self.cookie_expirations.get(url) is None) or (datetime.datetime.now() > self.cookie_expirations.get(url)) or (self.cookies is None)
        if not (force_create or need_to_update): return self.cookies

        if not force_create:
            # if we need to update, first load cookies from file (if it exists) and see if there is a valid one already
            # this actually requires that *all* cookies for the domain are valid (I don't understand why, but some have expirations of +1 day, others are +5 day,
            # and we care that the current time does not surpass the +1 day)
            self.cookies = cookielib.MozillaCookieJar()
            try:
                self.cookies.load(self.cookie_file, ignore_discard=True, ignore_expires=True)
            except IOError:
                print "Cookie file {} doesn't exist. It will be generated".format(self.cookie_file)
            except cookielib.LoadError:
                print "Cookie file {} couldn't be read. It will be (re)generated".format(self.cookie_file)
            domain = url.split("://",1)[1].split("/",1)[0]
            is_cookie_valid = True
            earliest_expiration = datetime.datetime.now()
            for c in self.cookies:
                # loop through cookies for this matching domain
                if c.domain != domain: continue
                # if any of them are expired, we break and end up making it later
                if c.is_expired():
                    is_cookie_valid = False
                    break
                # otherwise, store the oldest expiration date
                dt = datetime.datetime.fromtimestamp(c.expires)
                if dt < earliest_expiration: earliest_expiration = dt
            # if the earliest expiration is still more than a couple of days in the future, then this must be bogus.
            if earliest_expiration - datetime.datetime.now() > datetime.timedelta(hours=48):
                is_cookie_valid = False
            if is_cookie_valid:
                self.cookie_expirations[url] = earliest_expiration
                return self.cookies

        # if there isn't a valid cookie in the file, we must run this cern command and reload the file
        cmd = "cern-get-sso-cookie --cert {usercert} --key {userkey} -r -o {cookiefile} -u {url}".format(
                usercert = self.usercert,
                userkey = self.userkey_passwordless,
                cookiefile = self.cookie_file,
                url = url,
                )
        print "Making cookie with cern-get-sso-cookie: {}".format(cmd)
        os.system(cmd)

        # actually, we would want to load the file first, find the relevant cookie, and get the proper
        # expiration date. BUT we already know that cern SSO cookies expire after 12 hours, so don't
        # waste time on a loop
        self.cookie_expirations[url] = datetime.datetime.now()+datetime.timedelta(hours=12)
        self.cookies = cookielib.MozillaCookieJar()
        self.cookies.load(self.cookie_file, ignore_discard=True, ignore_expires=True)
        return self.cookies

    def get_request(self,url,params={},which="get"):
        cookies = []

        # non-public APIs need SSO (contrast with /mcm/public/restapi)
        if "/mcm/restapi/" in url:
            cookies = self.get_sso_cookies(url=API_URLS["mcm_private_url"])
        if "/pmp/api/" in url:
            cookies = self.get_sso_cookies(url=API_URLS["pmp_url"])
        if "/xsdb/api/" in url:
            cookies = self.get_sso_cookies(url=API_URLS["xsdb_url"])

        if which == "get":
            r = self.session.get(
                    url,
                    params=params,
                    cookies=cookies,
                    timeout=30,
                    )
        elif which == "post":
            r = self.session.post(
                    url,
                    data=params,
                    cookies=cookies,
                    timeout=30,
                    )
        else:
            raise RuntimeError("What kind of request is: {}?".format(which))
        return r



class BaseApi(object):

    def __init__(self,fetcher=None,db=None):
        self.fetcher = fetcher
        self.db = db
        self.url_stack = []
        self.elapsed_stack = []
        self.fromcache_stack = []

    def make_response(self,data={},status="success"):
        return dict(
                urls=self.url_stack,
                request_times=self.elapsed_stack,
                from_cache=self.fromcache_stack,
                status=status,
                payload=data,
                )

    def maybe_raise_exception(self, r, service, message=None):
        if not r.ok: 
            raise RuntimeError("Exception from {} ({}): {}".format(service,r.url,(message() if message is not None else r.reason)))
        else:
            pass

    def update_url_stack(self,r):
        self.url_stack.append(r.url)
        self.elapsed_stack.append(r.elapsed.total_seconds())
        self.fromcache_stack.append(getattr(r,"from_cache",False))


class DBSApi(BaseApi):

    def get_dataset_event_count(self,dataset):
        url = "{}/filesummaries".format(API_URLS["dbs_user_url"] if dataset.endswith("/USER") else API_URLS["dbs_global_url"])
        params = dict(dataset=dataset, validFileOnly=1)
        r = self.fetcher.get_request(url,params=params)
        self.update_url_stack(r)
        js = r.json()
        self.maybe_raise_exception(r,"DBS",lambda x:js["message"])
        j = js[0]
        return self.make_response(
                data=dict(
                    nevents=j["num_event"],
                    nfiles=j["num_file"],
                    nlumis=j["num_lumi"],
                    filesizeGB=round(j["file_size"]/1.e9,2),
                    )
                )

    def get_dataset_runs(self,dataset):
        url = "{}/runs".format(API_URLS["dbs_user_url"] if dataset.endswith("/USER") else API_URLS["dbs_global_url"])
        params = dict(dataset=dataset)
        r = self.fetcher.get_request(url,params=params)
        self.update_url_stack(r)
        js = r.json()
        self.maybe_raise_exception(r,"DBS",lambda x:js["message"])
        return self.make_response(
                data=js[0]["run_num"],
                )

    def get_dataset_config(self,dataset):
        url = "{}/outputconfigs".format(API_URLS["dbs_user_url"] if dataset.endswith("/USER") else API_URLS["dbs_global_url"])
        params = dict(dataset=dataset)
        r = self.fetcher.get_request(url,params=params)
        self.update_url_stack(r)
        js = r.json()
        self.maybe_raise_exception(r,"DBS",lambda x:js["message"])
        info = js[0]
        if "creation_date" in info:
            info["creation_date"] = str(datetime.datetime.fromtimestamp(info["creation_date"]))
        return self.make_response(
                data=info,
                )

    def get_dataset_files(self,dataset, run_num=None,lumi_list=[],selectors=[],max_files=None,to_dict=False):
        url = "{}/files".format(API_URLS["dbs_user_url"] if dataset.endswith("/USER") else API_URLS["dbs_global_url"])
        params = dict(
                dataset=dataset,
                validFileOnly=(0 if any(x in selectors for x in ["any","all","invalid","production"]) else 1),
                detail=1,
                )
        r = self.fetcher.get_request(url,params=params)
        self.update_url_stack(r)
        js = r.json()
        self.maybe_raise_exception(r,"DBS",lambda x:js["message"])
        files = [[f["logical_file_name"], f["event_count"], f["file_size"]/1.0e9] for f in js[:max_files]]
        if to_dict:
            files = [dict(name=f[0],nevents=f[1],sizeGB=f[2]) for f in files]
        return self.make_response(
                data=files,
                )

    def get_list_of_datasets(self,dataset_wildcarded, short=True, selectors=[]):
        if dataset_wildcarded.count("/") != 3:
            raise RuntimeError("You need three / in your dataset query")

        _, pd, proc, tier = dataset_wildcarded.split("/")

        url = "{}/datasets".format(API_URLS["dbs_user_url"] if dataset_wildcarded.endswith("/USER") else API_URLS["dbs_global_url"])
        params = dict(
                primary_ds_name=pd,
                processed_ds_name=proc,
                data_tier_name=tier,
                detail=1,
                )
        if any(x in selectors for x in ["any","all","invalid","production"]):
            params["dataset_access_type"] = "*"

        r = self.fetcher.get_request(url,params=params)
        self.update_url_stack(r)
        js = r.json()
        self.maybe_raise_exception(r,"DBS",lambda x:js["message"])

        datasets = []
        if short:
            datasets = [j["dataset"] for j in js]
        else:
            if len(js) > 30:
                raise RuntimeError(
                        "Getting detailed information for all {} datasets will take too long".format(len(js))
                        )
            for j in js:
                info = self.get_dataset_event_count(j["dataset"])["payload"]
                info["dataset"] = j["dataset"]
                datasets.append(info)

        return self.make_response(
                data=datasets,
                )

    def get_dataset_parents(self,dataset):
        def get_parent(ds):
            url = "{}/datasetparents".format(API_URLS["dbs_user_url"] if dataset.endswith("/USER") else API_URLS["dbs_global_url"])
            params = dict(dataset=ds)
            r = self.fetcher.get_request(url,params=params)
            self.update_url_stack(r)
            js = r.json()
            self.maybe_raise_exception(r,"DBS",lambda x:js["message"])
            if not js: return None
            return js[0].get("parent_dataset")
        parents = []
        ds = dataset
        for i in range(4):
            ds = get_parent(ds)
            if not ds: break
            parents.append(ds)
        return self.make_response(
            data=parents,
            )

    def get_arbitrary_url(self,url):
        r = self.fetcher.get_request(url)
        self.update_url_stack(r)
        js = r.json()
        self.maybe_raise_exception(r,"DBS",lambda x:js["message"])
        return self.make_response(data=js)

class PhedexApi(BaseApi):

    def get_file_replicas(self,filename,typ="lfn"):
        url = "{}/fileReplicas".format(API_URLS["phedex_url"])
        params = {}
        params[typ] = filename
        r = self.fetcher.get_request(url,params=params)
        self.update_url_stack(r)
        js = r.json()
        self.maybe_raise_exception(r,"Phedex")
        block = js["phedex"]["block"]
        if typ == "lfn":
            block = block[0]
        return self.make_response(data={"block": block})


    def get_dataset_replica_fractions(self,dataset):
        url = "{}/blockReplicas".format(API_URLS["phedex_url"])
        params = dict(dataset=dataset)
        r = self.fetcher.get_request(url,params=params)
        js = r.json()
        self.update_url_stack(r)
        self.maybe_raise_exception(r,"Phedex")
        blocks = js["phedex"]["block"]
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
        js = {"dataset": dataset, "site_fractions": sorted(sitepairs,key=lambda k: k[1], reverse=True)}
        return self.make_response(data=js)

class MCMApi(BaseApi):

    def get_from_x(self,thing,which="dataset",include_driver=False,slim_json=False):
        if which == "dataset":
            url = "{}/requests/produces/{}".format(API_URLS["mcm_public_url"],thing[1:])
        elif which == "chain":
            url = "{}/chained_requests/get/{}".format(API_URLS["mcm_private_url"],thing)
        elif which == "request":
            url = "{}/requests/get/{}".format(API_URLS["mcm_public_url"],thing)
        r = self.fetcher.get_request(url)
        self.update_url_stack(r)
        js = r.json()
        self.maybe_raise_exception(r,"McM")
        if include_driver:
            js["results"]["driver"] = self.get_setup_from_request(js["results"]["_id"])["payload"]
        js = js["results"]
        if slim_json:
            new_js = {}
            if "generator_parameters" in js:
                for k in ["cross_section","filter_efficiency","match_efficiency"]:
                    if len(js["generator_parameters"]):
                        new_js[k] = js["generator_parameters"][-1][k]
            for k in ["cmssw_release","status","fragment","driver","prepid","notes"]:
                new_js[k] = js.get(k)
            new_js["prepid"] = js.get("_id",js.get("prepid"))
            js = new_js
        return self.make_response(data=js)

    def get_setup_from_request(self,request):
        url = "{}/requests/get_setup/{}".format(API_URLS["mcm_public_url"],request)
        r = self.fetcher.get_request(url)
        self.update_url_stack(r)
        ret = r.content
        self.maybe_raise_exception(r,"McM")
        return self.make_response(data=ret)

    def get_driver_chain_from_dataset(self,dataset, first_only=True):
        js = self.get_from_x(dataset,which="dataset")["payload"]
        combchains = js["member_of_chain"]
        js = self.get_from_x(combchains[-1],which="chain")["payload"]
        chains = js["chain"]
        if first_only:
            info = self.get_from_x(chains[0],which="request",include_driver=True,slim_json=True)["payload"]
        else:
            info = [self.get_from_x(request,which="request",include_driver=True,slim_json=True)["payload"]
                    for request in chains]
        return self.make_response(data=info)

class PMPApi(BaseApi):

    def get_pmp_campaign_info(self,campaign):
        url = "{}/historical".format(API_URLS["pmp_url"])
        params = dict(
                r=campaign,
                granularity=1,
                )
        r = self.fetcher.get_request(url,params=params)
        self.update_url_stack(r)
        js = r.json()
        self.maybe_raise_exception(r,"PMP")
        info = {}
        info["valid_tags"] = js["results"]["valid_tags"]
        info["invalid_tags"] = js["results"]["invalid_tags"]
        submitted_requests = []
        for sr in js["results"]["submitted_requests"]:
            submitted_requests.append(dict(
                prepid=sr["r"],
                priority=sr["p"],
                pct_done=round(100.0*sr["d"]/sr["x"],1),
                exp_events=sr["x"],
                dataset=sr.get("ds",""),
                ))
        info["requests"] = submitted_requests
        return self.make_response(data=info)

class SNTApi(BaseApi):

    def get_samples(self, entity, selectors=[], short=True):

        if "," in entity:
            entity,x = entity.split(",",1)
            selectors.extend(x.split(","))

        match_dict = {"dataset_name": entity}

        # if we didn't specify a sample_type, then assume we only want CMS3 and not BABY
        if not any(map(lambda x: "sample_type" in x, selectors)):
            selectors.append("sample_type=CMS3")

        if selectors:
            for more in selectors:
                key = more.split("=")[0].strip()
                val = more.split("=")[1].strip()
                match_dict[key] = val

        samples = self.db.fetch_samples_matching(match_dict)
        samples = sorted(samples, key = lambda x: x.get("cms3tag","")+str(x.get("nevents_out","0")), reverse=True)

        if short:
            new_samples = []
            for sample in samples:
                for key in ["sample_id","filter_type","assigned_to", \
                            "comments","twiki_name", "analysis", "baby_tag"]:
                    del sample[key]
                new_samples.append(sample)
            samples = new_samples

        return self.make_response(data=samples)

    def update_sample(self, entity):
        s = {}
        for keyval in entity.split(","):
            key,val = keyval.strip().split("=")
            s[key] = val
        did_update = self.db.update_sample(s)
        s["updated"] = did_update
        self.make_response(self,data=s,status=("success" if did_update else "fail"))

    def delete_sample(self, entity):
        s = {}
        for keyval in entity.split(","):
            key,val = keyval.strip().split("=")
            s[key] = val
        did_delete = self.db.delete_sample(s)
        s["deleted"] = did_delete
        self.make_response(self,data=s,status=("success" if did_delete else "fail"))

class XSDBApi(BaseApi):

    def get_samples(self,pattern):
        # convert from glob-style to valid regex
        pattern = pattern.replace("/",".").replace("*",".*")
        url = "https://cms-gen-dev.cern.ch/xsdb/api/search"
        params = '{"search":{"DAS":"%s"},"pagination":{"pageSize":0}}' % pattern
        url = "{}/search".format(API_URLS["xsdb_url"])
        r = self.fetcher.get_request(url,params=params,which="post")
        self.update_url_stack(r)
        js = r.json()
        self.maybe_raise_exception(r,"XSDB")
        return self.make_response(data=js)


# enable_requests_caching("test3",expire_after=3000)
f = Fetcher()
db = SNTDBInterface(fname="allsamples.db")

def do_query(query, query_type, short=True):

    entity, selectors, pipes = transform_input(query)

    if query_type == "snt": ret = SNTApi(db=db).get_samples(entity, selectors, short)

    elif query_type == "basic":
        if "*" in entity:
            ret = DBSApi(fetcher=f).get_list_of_datasets(entity, short=short)
        else:
            ret = DBSApi(fetcher=f).get_dataset_event_count(entity)

    elif query_type == "files": ret = DBSApi(fetcher=f).get_dataset_files(entity, selectors=selectors, max_files=(10 if short else None), to_dict=True)

    elif query_type == "runs": ret = DBSApi(fetcher=f).get_dataset_runs(entity)

    elif query_type == "config": ret = DBSApi(fetcher=f).get_dataset_config(entity)

    elif query_type == "mcm": ret = MCMApi(fetcher=f).get_driver_chain_from_dataset(entity, first_only=True)

    elif query_type == "parents": ret = DBSApi(fetcher=f).get_dataset_parents(entity)

    elif query_type == "chain": ret = MCMApi(fetcher=f).get_driver_chain_from_dataset(entity, first_only=False)

    elif query_type == "update_snt": ret = SNTApi(db=db).update_sample(entity,selectors)

    elif query_type == "delete_snt": ret = SNTApi(db=db).delete_sample(entity,selectors)

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

if __name__ == "__main__":

    # print do_query("/TTTT_*,cms3tag=CMS4_V10*",query_type="snt")
    # print do_query("/TTTT_*/*/MINIAODSIM",query_type="basic")
    # ds = do_query("/TTTT_*/*/MINIAODSIM",query_type="basic")["payload"][0]
    # print do_query(ds,query_type="mcm")
    # print do_query(ds,query_type="parents")
    # print do_query("{} | grep sizeGB | stats".format(ds),query_type="files")
    # print do_query("/DYJets_*94X* | grep eff_lumi,scale1fb | sort | grep scale1fb | stats",query_type="snt")


    # times = []
    # for _ in range(100):
    #     t0 = time.time()
    #     js = SNTApi(db=sntdb).get_samples("/TTTT_*")
    #     # pprint(js)
    #     t1 = time.time()
    #     print t1-t0
    #     times.append(t1-t0)
    #     sntdb.connection.commit()
    #     # sntdb.close()
    # print times
    # print sum(times)/len(times)
    # print sum(times[1:])/len(times[1:])

    f = Fetcher()
    # pprint(MCMApi(fetcher=f).get_driver_chain_from_dataset("/TTTT_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18DRPremix-102X_upgrade2018_realistic_v15_ext1-v2/AODSIM"))
    # pprint(PMPApi(fetcher=f).get_pmp_campaign_info("RunIIAutumn18FSPremix"))
    # pprint(PhedexApi(fetcher=f).get_dataset_replica_fractions("/TTTT_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM"))

    # pprint(MCMApi(fetcher=f).get_samples("/TTJets*/*94X*/MINIAODSIM"))
    # pprint(MCMApi(fetcher=f).get_driver_chain_from_dataset("/TTTT_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18DRPremix-102X_upgrade2018_realistic_v15_ext1-v2/AODSIM"))
    print f.cookies
    print f.get_sso_cookies(API_URLS["xsdb_url"])
    print f.cookie_expirations
    # pprint(XSDBApi(fetcher=f).get_samples("/TTJets*/*94X*/MINIAODSIM"))
    _ = XSDBApi(fetcher=f).get_samples("/TTJets*/*94X*/MINIAODSIM")
    # print f.get_sso_cookies(API_URLS["pmp_url"], force_create=True)
    # print f.get_sso_cookies(API_URLS["pmp_url"], force_create=False)
    print f.cookie_expirations
    # pprint(PMPApi(fetcher=f).get_pmp_campaign_info("RunIIAutumn18FSPremix"))

    # for c in f.cookies:
    #     print c.domain, c.is_expired(), datetime.datetime.fromtimestamp(c.expires)
        # if c.domain != domain: continue
        # # if c.is_expired(): continue
        # self.cookie_expirations[url] = datetime.datetime.fromtimestamp(c.expires)
        # return self.cookies


    # # print list(cookies)[-1].is_expired()
    # # print dir(list(cookies)[-1])
    # # last = list(cookies)[-1]
    # # for d in dir(last):
    # #     print d, getattr(last,d)
    # # ['__doc__', '__init__', '__module__', '__repr__', '__str__', '_rest', 'comment', 'comment_url', 'discard', 'domain', 'domain_initial_dot', 'domain_specified', 'expires', 'get_nonstandard_attr', 'has_nonstandard_attr', 'is_expired', 'name', 'path', 'path_specified', 'port', 'port_specified', 'rfc2109', 'secure', 'set_nonstandard_attr', 'value', 'version']

    # f = Fetcher()
    # sntdb = DBInterface(fname="allsamples.db")
    # pprint(SNTApi(db=sntdb).get_samples("/TTTT_*,cms3tag=CMS4_V10*"))
    # pprint(DBSApi(fetcher=f).get_dataset_config("/TTTT_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18DRPremix-102X_upgrade2018_realistic_v15_ext1-v2/AODSIM"))
    # pprint(DBSApi(fetcher=f).get_dataset_event_count("/SingleElectron/Run2016B-PromptReco-v1/MINIAOD"))
    # pprint(DBSApi(fetcher=f).get_dataset_parents("/TTTT_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM"))
    # pprint(DBSApi(fetcher=f).get_list_of_datasets("/SingleElectron/Run2016*-PromptReco-v1/MINIAOD"))
    # pprint(MCMApi(fetcher=f).get_driver_chain_from_dataset("/TTTT_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18DRPremix-102X_upgrade2018_realistic_v15_ext1-v2/AODSIM"))
    # pprint(MCMApi(fetcher=f).get_first_in_chain_from_dataset("/TTTT_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM"))
    # pprint(MCMApi(fetcher=f).get_from_x("/TTTT_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM", which="dataset",include_driver=True,slim_json=True))
    # pprint(PMPApi(fetcher=f).get_pmp_campaign_info("RunIIAutumn18FSPremix"))
    # pprint(PhedexApi(fetcher=f).get_dataset_replica_fractions("/TTTT_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM"))
    # pprint(XSDBApi(fetcher=f).get_samples("/TTJets*/*94X*/MINIAODSIM"))

