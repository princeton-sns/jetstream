#!/bin/bash

echo "Running python tests"
# Using explicit paths for now (alternatively could use __init__.py files)
export PYTHONPATH=`pwd`/src/python/jetstream':'`pwd`'/src/proto/python/':$PYTHONPATH  #should absolute-ize?
echo 'PYTHONPATH = ' $PYTHONPATH

# Python 2.7 has auto-discovery for unit tests.
# SS: Can't get this to find the tests, also we should support 2.6 if possible

PY_27=`python --version 2>&1 | grep -cq 2.7`
if $PY_27; then
  python -m unittest discover -s 'src/python/jetstream/int_tests' -p '*_test.py'
else
  for t in `find src/python/jetstream/int_tests -name *_test.py`; do
    echo $t
    python $t
  done
fi