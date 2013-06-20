#!/bin/bash
git pull
cmake .
make -j3
cp jsnoded ~/jetstream
rm -r ~/jetstream/src/proto
cp -r src/proto ~/jetstream/src/proto
cp `find . -name '*.so'` /home/asrabkin/jetstream/lib
