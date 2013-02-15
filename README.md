# Software requirements:
#
#   -- Google Protobufs (2.4.1+, protobuf, protobuf-devel, protobuf-compiler)
#      - Separate installation for C++ and python
#      - On Ubuntu build/install from sources (http://code.google.com/p/protobuf/downloads/)
#   -- Python (2.6+)
#   -- BOOST (including system, program-options, ...)
#   -- Google Test Framework.  Installs via macports as google-test
#   -- CMAKE (2.x?)
#   -- Google Glog (0.3.2+)  Can be installed as google-glog via macports.

We currently also require the Oracle MySQL Connector/C++.  NOTE THAT THIS IS UNDER GPL (v2)!!

To install, you need to:
cp src/mysql_udfs/merge/libjsmysqludfs_merge.dylib /opt/local/lib/mysql5/mysql/plugin/