from db import DBInterface
import time


def do_test():
    sample = {
                "dataset_name": "/DYJetsToLL_M-10to50_TuneCUETP8M1_13TeV-amcatnloFXFX-pythia8/RunIISpring15MiniAODv2-74X_mcRun2_asymptotic_v2-v1/MINIAODSIM",
                "twiki_name": "Run2SamplesReMINIAOD_25ns",
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
                "assigned_to": "Rafael",
                "comments": "xsec from MCM"
                }

    # :memory: uses RAM instead of saving a file
    # db = DBInterface(fname=":memory:")
    db = DBInterface(fname=":memory:")

    # empty DB and re-initialize table
    db.drop_table()
    db.make_table()

    # make some more fake samples
    sample1 = sample.copy()

    sample2 = sample.copy()
    sample2["cms3tag"] = "the_tag_2"

    sample3 = sample.copy()
    sample3["cms3tag"] = "the_tag_3"
    sample3["nevents_out"] = 12345


    # insert samples
    db.update_sample(sample1)
    db.update_sample(sample2)
    db.update_sample(sample3)


    # next line shouldn't add a new row since there's duplicate checking
    db.update_sample(sample3)


    assert( len(db.fetch_samples_matching({"sample_type":"CMS3"})) == 3 ) # should match all three samples
    assert( len(db.fetch_samples_matching({"cms3tag":"the_tag_2"})) == 1 ) # should match only sample2
    assert( len(db.fetch_samples_matching({"nevents_out":12345})) == 1 ) # should match only sample3
    assert( len(db.fetch_samples_matching({"cms3tag":"the_tag_3","nevents_out":12345})) == 1 ) # should match only sample3

    # get sample2, change the evt count (leaving the rest), update it
    sample = db.fetch_samples_matching({"cms3tag":"the_tag_2"})[0]
    sample["nevents_out"] = 666
    db.update_sample(sample)

    assert( len(db.fetch_samples_matching({"cms3tag":"the_tag_2","nevents_out":666})) == 1 )
    assert( len(db.fetch_samples_matching({"sample_type":"CMS3"})) == 3 ) # still only have 3 samples
    assert( len(db.read_to_dict_list("select * from sample")) == 3 ) # check that this method returns all samples

    assert( len(db.fetch_samples_matching({"cms3tag":"the_tag_4"})) == 0 ) # shouldn't match anything


    # if we change the cms3tag and try to update the sample, it's actually a new sample, so another row gets added
    sample = db.fetch_samples_matching({"cms3tag":"the_tag_2"})[0]
    sample["cms3tag"] = "the_tag_4"
    db.update_sample(sample)

    assert( len(db.fetch_samples_matching({"cms3tag":"the_tag_4"})) == 1 ) # should now match one sample
    assert( len(db.fetch_samples_matching({"sample_type":"CMS3"})) == 4 ) # and we have 4 rows now


    assert( db.fetch_samples_matching({"duck":1}) == [] ) # unrecognized key returns nothing
    assert( db.fetch_samples_matching({}) == [] ) # empty selection dict returns nothing

    assert( db.update_sample({}) == False ) # don't do anything if empty
    assert( db.update_sample({"duck":1}) == False ) # don't do anything if unrecognized key

    assert( len(db.fetch_samples_matching({"cms3tag":"*_tag_*"})) == 3 ) # check wildcard support


    db.close()

    # print "Calculations correct"

    return True

