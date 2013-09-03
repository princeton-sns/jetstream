#!/bin/bash

RUN=903_group
mkdir $RUN
scp sns48:/disk/local/asr_jetstream/image_quality.out $RUN/sns48_$RUN.log
scp princeton_jetstream@node20.mpisws.vicci.org:/jetstream/log.out $RUN/mpi20_$RUN.log
scp princeton_jetstream@node35.gt.vicci.org:/jetstream/log.out $RUN/gt35_$RUN.log
scp princeton_jetstream@node20.gt.vicci.org:/jetstream/log.out $RUN/gt20_$RUN.log
echo "Done Fetching"
python data_analysis/plot_local_deg.py $RUN/gt35_$RUN.log GT35

python data_analysis/plot_local_deg.py $RUN/gt20_$RUN.log GT20

python data_analysis/plot_local_deg.py $RUN/mpi20_$RUN.log MPI20

python data_analysis/latency_over_time.py $RUN/sns48_$RUN.log
