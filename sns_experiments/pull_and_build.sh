#!/bin/bash
git pull
cmake .
make
cp jsnoded ~/jetstream
cp `find . -name '*.so'` /home/asrabkin/jetstream/lib
