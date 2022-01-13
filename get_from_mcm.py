#!/usr/bin/env python

from __future__ import print_function

import json
try:
    from urllib2 import urlopen
    from urllib import urlencode
except:
    # python3 compatibility
    from urllib.request import urlopen
    from urllib.parse import urlencode
import sys
import argparse
import socket
import time
import glob

import requests
import cookielib
import datetime
#BASEURL = "http://localhost:8891/dis/serve"

import os

from config import API_URLS, AUTH_PATHS
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
                print ("Cookie file {} doesn't exist. It will be generated".format(self.cookie_file) )
            except cookielib.LoadError:
                print ("Cookie file {} couldn't be read. It will be (re)generated".format(self.cookie_file) ) 
            domain = url.split("://",1)[1].split("/",1)[0]
            is_cookie_valid = (len(self.cookies) > 0)
            earliest_expiration = datetime.datetime.now() + datetime.timedelta(hours=999)
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
            if earliest_expiration - datetime.datetime.now() < datetime.timedelta(hours=15):
                is_cookie_valid = False
            if is_cookie_valid:
                self.cookie_expirations[url] = earliest_expiration
                return self.cookies

        # if there isn't a valid cookie in the file, we must run this cern command and reload the file
        cmd = "cern-get-sso-cookie --cert {usercert} --key {userkey} -r -o {cookiefile} -u {url} {extra}".format(
                usercert = self.usercert,
                userkey = self.userkey_passwordless,
                cookiefile = self.cookie_file,
                url = url,
                extra = AUTH_PATHS["extra_args_sso"],
                )
        print ("Making cookie with cern-get-sso-cookie: {}".format(cmd) )
        os.system(cmd)

        # actually, we would want to load the file first, find the relevant cookie, and get the proper
        # expiration date. BUT we already know that cern SSO cookies expire after 12 hours, so don't
        # waste time on a loop
        self.cookie_expirations[url] = datetime.datetime.now()+datetime.timedelta(hours=6)
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
        self.maybe_raise_exception(r,"McM")
        js = r.json()
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
        self.maybe_raise_exception(r,"McM")
        ret = r.content
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

if __name__ == '__main__':

    f = Fetcher()
    #entity = "/GluGluHToGG_M125_13TeV_amcatnloFXFX_pythia8/RunIIFall17NanoAODv7-PU2017_12Apr2018_Nano02Apr2020_EXT_102X_mc2017_realistic_v8-v1/NANOAODSIM"
    #ret = MCMApi(fetcher=f).get_from_x(entity, which="dataset", include_driver=True)
    entity = "HIG-chain_RunIIFall17wmLHEGS_flowRunIIFall17DRPremixPU2017_flowRunIIFall17MiniAODv2_flowRunIIFall17NanoAOD-00897"
    ret = MCMApi(fetcher=f).get_from_x(entity, which="chain", include_driver=False)
    #entity = "HIG-RunIISummer20UL16wmLHEGEN-03485"
    #ret = MCMApi(fetcher=f).get_from_x(entity, which="request", include_driver=False)

    from utils import pprint
    pprint(ret)
