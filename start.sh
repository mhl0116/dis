#!/usr/bin/env sh

# Need python2.7 from CMSSW
source /cvmfs/cms.cern.ch/cmsset_default.sh
cd /cvmfs/cms.cern.ch/slc6_amd64_gcc630/cms/cmssw/CMSSW_9_4_9; 
cmsenv; 
cd -; 
export LANG=en_US.UTF-8


# Now make our virtualenv for python packages separate from CMSSW
# CMSSW still creeps in (wtf) so need two exports
[ -d virtualenv ] || pip install virtualenv --target=`pwd`/virtualenv virtualenv
export PYTHONPATH=`pwd`/virtualenv:$PYTHONPATH
[ -d myenv ] || python virtualenv/virtualenv.py myenv
source myenv/bin/activate
export PYTHONPATH=`pwd`/myenv/lib/python2.7/site-packages:$PYTHONPATH
pip install requests requests-cache flask pygments

python serve.py | tee -a log.txt
