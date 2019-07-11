import unittest
import os
import time
import logging

from api import Fetcher, \
        DBSApi, PhedexApi, MCMApi, \
        SNTApi, PMPApi, XSDBApi
from snt_db import SNTDBInterface
from utils import enable_requests_caching, pprint

class SNTApiTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.api = SNTApi(db=SNTDBInterface(fname="allsamples.db"))

    def test_get_samples(self):
        out = self.api.get_samples("/TTTT_*,cms3tag=CMS4_V10*")
        nsamples = len(out["payload"])
        self.assertGreater(nsamples,0)

    def test_multiple_constraints(self):
        out = self.api.get_samples("/TTTT_*,cms3tag=CMS4_V10*,gtag=102X*")
        nsamples = len(out["payload"])
        self.assertGreater(nsamples,0)


class DBSApiTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.f = Fetcher()

    def setUp(self):
        self.api = DBSApi(fetcher=self.f)

    def test_get_dataset_config(self):
        out = self.api.get_dataset_config("/TTTT_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18DRPremix-102X_upgrade2018_realistic_v15_ext1-v2/AODSIM")
        self.assertEqual(out["payload"]["release_version"], "CMSSW_10_2_5")
        self.assertEqual(out["payload"]["global_tag"], "102X_upgrade2018_realistic_v15")

    def test_get_dataset_event_count(self):
        out = self.api.get_dataset_event_count("/SingleElectron/Run2016B-PromptReco-v1/MINIAOD")
        self.assertGreater(out["payload"]["nevents"], 1e6)
        self.assertGreater(out["payload"]["nlumis"], 5e3)
        self.assertGreater(out["payload"]["filesizeGB"], 10)
        self.assertEqual(len(out["urls"]),1)

    def test_get_dataset_parents(self):
        out = self.api.get_dataset_parents("/TTTT_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM")
        self.assertEqual(out["payload"][0].rsplit("/",1)[-1],"AODSIM")
        self.assertEqual(len(out["urls"]),2)

    def test_get_list_of_datasets_short(self):
        out = self.api.get_list_of_datasets("/SingleElectron/Run2016*-PromptReco-v1/MINIAOD",short=True)
        self.assertEqual(len(out["urls"]),1)
        self.assertGreater(len(out["payload"]),1)

    def test_get_list_of_datasets_long(self):
        out = self.api.get_list_of_datasets("/SingleElectron/Run2016*-PromptReco-v1/MINIAOD",short=False)
        self.assertGreater(len(out["payload"]),1)
         # one http request expands the *; the rest get information about a given dataset
        self.assertEqual(len(out["urls"])-1,len(out["payload"]))
        self.assertEqual(out["payload"][0]["dataset"].rsplit("/",1)[-1],"MINIAOD")
        nfiles = [s["nfiles"] for s in out["payload"]]
        self.assertGreater(sum(nfiles),1000)

    def test_get_dataset_runs(self):
        out = self.api.get_dataset_runs("/SingleElectron/Run2016B-PromptReco-v1/MINIAOD")
        self.assertGreater(len(out["payload"]),40)
        self.assertGreater(min(out["payload"]),270e3)
        self.assertLess(max(out["payload"]),275e3)

    def test_get_dataset_files(self):
        out = self.api.get_dataset_files("/SingleElectron/Run2017B-PromptReco-v1/MINIAOD")
        files = out["payload"]
        self.assertGreater(len(files),30)
        self.assertTrue(files[0][0].startswith("/store/"))
        self.assertGreater(files[0][2],0.0)

        out = self.api.get_dataset_files("/SingleElectron/Run2016B-PromptReco-v1/MINIAOD",max_files=10)
        files = out["payload"]
        self.assertEqual(len(files),10)

        out = self.api.get_dataset_files("/SingleElectron/Run2016B-PromptReco-v1/MINIAOD",to_dict=True)
        files = out["payload"]
        self.assertItemsEqual(files[0].keys(),["name","nevents","sizeGB"])

    def test_get_arbitrary_url(self):
        out1 = self.api.get_dataset_event_count("/SingleElectron/Run2016B-PromptReco-v1/MINIAOD")
        url1 = out1["urls"][0]
        out2 = self.api.get_arbitrary_url(url1)
        self.assertEqual(out1["payload"]["nfiles"],out2["payload"][0]["num_file"])


class PhedexApiTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.f = Fetcher()

    def setUp(self):
        self.api = PhedexApi(fetcher=self.f)

    def test_get_single_file_replicas(self):
        lfn = "/store/data/Run2018C/MuonEG/MINIAOD/17Sep2018-v1/100000/C210B8A7-BCEE-2744-88D1-10D2C1231161.root"
        out = self.api.get_file_replicas(lfn,typ="lfn")
        block = out["payload"]["block"]
        finfo = block["file"][0]
        sites = [replica["node"] for replica in finfo["replica"]]
        self.assertTrue(all([s.startswith("T") for s in sites]))
        self.assertEqual(finfo["name"],lfn)

    def test_get_dataset_file_replicas(self):
        out = self.api.get_file_replicas("/MuonEG/Run2018C-17Sep2018-v1/MINIAOD",typ="dataset")
        blocks = out["payload"]["block"]
        self.assertGreater(len(blocks),10)
        self.assertGreater(len(blocks[0]),0)

    def test_get_dataset_replica_fractions(self):
        out = self.api.get_dataset_replica_fractions("/MuonEG/Run2018C-17Sep2018-v1/MINIAOD")
        site_fractions = out["payload"]["site_fractions"]
        sites = zip(*site_fractions)[0]
        self.assertTrue(all([s.startswith("T") for s in sites]))

class PMPApiTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.f = Fetcher()

    def setUp(self):
        self.api = PMPApi(fetcher=self.f)

    def test_get_pmp_campaign_info(self):
        out = self.api.get_pmp_campaign_info("RunIIAutumn18FSPremix")
        self.assertGreater(len(out["payload"]["requests"]),10)


class XSDBApiTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.f = Fetcher()

    def setUp(self):
        self.api = XSDBApi(fetcher=self.f)

    def test_get_samples(self):
        out = self.api.get_samples("/TTJets*/*94X*/MINIAODSIM")
        self.assertGreater(len(out["payload"]),5)
        self.assertGreater(float(out["payload"][0]["cross_section"]),50.)

class MCMApiTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.f = Fetcher()

    def setUp(self):
        self.api = MCMApi(fetcher=self.f)
        self.dataset = "/TTTT_TuneCP5_13TeV-amcatnlo-pythia8/RunIIAutumn18MiniAOD-102X_upgrade2018_realistic_v15_ext1-v2/MINIAODSIM"
        self.prepid = "TOP-RunIIAutumn18MiniAOD-00028"
        self.chainid = "TOP-chain_RunIIFall18wmLHEGS_flowRunIIAutumn18DRPremix_flowRunIIAutumn18MiniAOD_flowRunIIAutumn18NanoAODv4-00057"

    def test_get_setup_from_request(self):
        out = self.api.get_setup_from_request(self.prepid)
        self.assertTrue("#!/bin/bash" in out["payload"])

    def test_get_from_dataset(self):
        out = self.api.get_from_x(self.dataset, which="dataset", include_driver=True, slim_json=True)
        self.assertTrue("#!/bin/bash" in out["payload"]["driver"])
        self.assertTrue("RunII" in out["payload"]["prepid"])
        # 2 urls since we queried the prepid, and also queried the driver
        self.assertEqual(len(out["urls"]),2)

    def test_get_from_request(self):
        out = self.api.get_from_x(self.prepid, which="request",slim_json=True)
        self.assertEqual(self.prepid,out["payload"]["prepid"])

    def test_get_from_chain(self):
        out = self.api.get_from_x(self.chainid,which="chain",slim_json=False)
        self.assertTrue("chain" in out["payload"])
        chain = out["payload"]["chain"]
        self.assertEqual(len(self.chainid.split("_")[1:]),len(chain))

    def test_get_first_in_chain_from_dataset(self):
        out = self.api.get_driver_chain_from_dataset(self.dataset, first_only=True)
        self.assertTrue("import FWCore" in out["payload"]["fragment"])
        self.assertTrue("#!/bin/bash" in out["payload"]["driver"])
        # 4 queries (convert dataset->chainid, convert chainid->first prepid, get fragment, get driver)
        self.assertEqual(len(out["urls"]),4)

    def test_get_driver_chain_from_dataset(self):
        out = self.api.get_driver_chain_from_dataset(self.dataset, first_only=False)
        nsteps = len(self.chainid.split("_")[1:])
        self.assertEqual(len(out["payload"]),nsteps)
        self.assertTrue(all("#!/bin/bash" in x["driver"] for x in out["payload"]))
        self.assertTrue(all("cmssw_release" in x for x in out["payload"]))
        # dataset->chainid, chainid->list of prepids, for each prepid get info and driver -- so 1+1+2*nsteps queries
        self.assertEqual(len(out["urls"]),2+2*nsteps)

class SNTDBInterfaceTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # :memory: uses RAM instead of saving a file
        cls.db = SNTDBInterface(fname=":memory:")

        cls.sample = {
                    "dataset_name": "/DYJetsToLL_M-10to50_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/MINIAODSIM",
                    "sample_type": "CMS3",
                    "filter_type": "NoFilter",
                    "nevents_in": 30301601,
                    "nevents_out": 30301601,
                    "xsec": 18610,
                    "kfactor": 1.11,
                    "filter_eff": 1,
                    "baby_tag": "",
                    "analysis": "",
                    "timestamp": int(time.time()),
                    "gtag": "74X_mcRun2_asymptotic_v2",
                    "cms3tag": "CMS3_V07-04-11",
                    "location": "/hadoop/cms/store/group/snt/run2_25ns_MiniAODv2/DYJetsToLL_M-10to50_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8_RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/V07-04-11/",
                    "comments": "xsec from MCM"
                    }

    def setUp(self):
        # empty DB and re-initialize table
        self.db.drop_table()
        self.db.make_table()

    def test_noops(self):
        self.assertEqual(self.db.fetch_samples_matching({"duck":1}), []) # unrecognized key returns nothing
        self.assertEqual(self.db.fetch_samples_matching({}), []) # empty selection dict returns nothing
        self.assertEqual(self.db.update_sample({}), False) # don't do anything if empty
        self.assertEqual(self.db.update_sample({"soylent":1}), False) # don't do anything if unrecognized key

    def test_add_one_sample(self):
        sample = self.sample.copy()
        self.db.update_sample(sample)
        matches = self.db.fetch_samples_matching(sample)
        self.assertEqual(len(matches),1)
        matches = self.db.fetch_samples_matching({"dataset_name":"*"})
        self.assertEqual(len(matches),1)

    def test_add_unique_sample(self):
        sample1 = self.sample.copy()
        sample1["cms3tag"] = "tag1"
        sample2 = self.sample.copy()
        sample2["cms3tag"] = "tag2"
        self.db.update_sample(sample1)
        self.db.update_sample(sample2)
        matches = self.db.fetch_samples_matching({"dataset_name":"*"})
        self.assertEqual(len(matches),2)


    def test_add_duplicate_sample(self):
        sample1 = self.sample.copy()
        sample2 = self.sample.copy()
        self.db.update_sample(sample1)
        self.db.update_sample(sample2)
        matches = self.db.fetch_samples_matching({"dataset_name":"*"})
        self.assertEqual(len(matches),1)

        sample3 = self.sample.copy()
        sample3["cms3tag"] = "tag3"
        self.db.update_sample(sample3)
        matches = self.db.fetch_samples_matching({"dataset_name":2})

    def test_matching(self):
        sample1 = self.sample.copy()
        sample2 = self.sample.copy()
        sample3 = self.sample.copy()

        sample1["cms3tag"] = "tag1"

        sample2["cms3tag"] = "tag2"

        sample3["cms3tag"] = "tag3"
        sample3["nevents_out"] = 123

        self.db.update_sample(sample1)
        self.db.update_sample(sample2)
        self.db.update_sample(sample3)

        self.assertEqual(len(self.db.read_to_dict_list("select * from sample")),3)

        self.assertEqual(len(self.db.fetch_samples_matching({"dataset_name":"*"})),3)
        self.assertEqual(len(self.db.fetch_samples_matching({"sample_type":"CMS3"})),3)
        self.assertEqual(len(self.db.fetch_samples_matching({"cms3tag":"tag2"})),1)
        self.assertEqual(len(self.db.fetch_samples_matching({"cms3tag":"tag5"})),0)
        self.assertEqual(len(self.db.fetch_samples_matching({"cms3tag":"tag*"})),3)
        self.assertEqual(len(self.db.fetch_samples_matching({"cms3tag":"tag*","nevents_out":123})),1)
        self.assertEqual(len(self.db.fetch_samples_matching({"cms3tag":"tag3","nevents_out":123})),1)

if __name__ == "__main__":
    enable_requests_caching("testcache")
    unittest.main()

