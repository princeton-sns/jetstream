#!/bin/bash

HOSTSFILE=sns_experiments/nodes.txt
SSH_OPTS="-o StrictHostKeyChecking=no"
JS_DIR=/home/asrabkin/jetstream/
CMD="${JS_DIR}/sns_experiments/start_worker.sh"

cd $JS_DIR

#####  Start Controller

export PYTHONPATH='src/python/jetstream':'src/proto/python/':$PYTHONPATH
python src/python/jetstream/controller.py &
CTRL_HOST=`hostname`:3456
#####  Start workers


for node in `cat $HOSTSFILE`; do
echo "will start on $node"
ssh ${SSH_OPTS} $node ${CMD}
done
