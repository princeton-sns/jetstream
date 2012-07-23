#!/bin/bash

echo "Running python tests"
export PYTHONPATH=`dirname $0`  #should absolute-ize?
for t in `find . -name *_test.py`; do
  echo $t
  python $t
done
