#!/bin/bash

echo "Running python tests"
# Using explicit paths for now (alternatively could use __init__.py files)
export PYTHONPATH=`pwd`/jetstream':'`pwd`'/../proto/python/'  #should absolute-ize?
echo 'PYTHONPATH = ' $PYTHONPATH
for t in `find . -name *_test.py`; do
  echo $t
  python $t
done
