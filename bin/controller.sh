#!/bin/bash

JS=`dirname $0`/..
env PYTHONPATH="$JS/src/python/jetstream":"$JS/src/proto/python/":$PYTHONPATH python src/python/jetstream/controller.py $@

