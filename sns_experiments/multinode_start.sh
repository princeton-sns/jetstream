#!/bin/bash

HOSTSFILE=sns_experiments/nodes.txt
SSH_OPTS="-o StrictHostKeyChecking=no"
JS_DIR=/home/asrabkin/jetstream/
CMD="nohup ${JS_DIR}/jsnoded --start -C ${JS_DIR}/config/sns48.conf & "

cd $JS_DIR

#####  Start Controller

export PYTHONPATH='src/python/jetstream':'src/proto/python/':$PYTHONPATH
python src/python/jetstream/controller.py &
CTRL_HOST=`hostname`:3456
#####  Start workers


for node in `cat $HOSTSFILE`; do
echo "will start on $node"
ssh ${SSH_OPTS} $node $CMD -a $CTRL_HOST
done
