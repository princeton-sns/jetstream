#!/bin/bash

echo "Running python tests"
# Using explicit paths for now (alternatively could use __init__.py files)
export PYTHONPATH=`pwd`/jetstream':'`pwd`'/../proto/python/':$PYTHONPATH  #should absolute-ize?
echo 'PYTHONPATH = ' $PYTHONPATH

#Python 2.7 has auto-discovery for unit tests.
python -m unittest discover -p '*_test.py

#for t in `find . -name *_test.py`; do
#  echo $t
#  python $t
#done
