

HOSTSFILE=sns_experiments/nodes.txt
SSH_OPTS="-o StrictHostKeyChecking=no"
JS_DIR=/home/asrabkin/jetstream/
CMD="${JS_DIR}/bin/stop_worker.sh"

for node in `cat $HOSTSFILE`; do
echo "will stop on $node"
ssh ${SSH_OPTS} $node ${CMD}
done

