DEP_INSTALL_DIR=/disk/local/jetstream_deps
JS_DIR=/disk/local/jetstream

apt-get install cmake
#apt-get install libprotobuf-dev 
apt-get install libgtest0 libgtest-dev
apt-get install libgoogle-glog-dev
apt-get install libmysqlcppconn-dev

if USE_MY_PROTO; then
cd $DEP_INSTALL_DIR

wget http://archive.ubuntu.com/ubuntu/pool/main/p/protobuf/protobuf_2.4.1.orig.tar.gz
tar xzf protobuf_2.4.1.orig.tar.gz 
cd protobuf-2.4.1/
./configure --prefix=/disk/local/jetstream_deps
make

cd $JS_DIR
cmake -DUSE_CLANG=OFF -DBOOST_ROOT=/disk/local/boost_1_50_0/ -DBoost_NO_SYSTEM_PATHS=True -DPROTOBUF_LIBRARY=${DEP_INSTALL_DIR}/lib/libprotobuf.so -DPROTOBUF_INCLUDE_DIR=${DEP_INSTALL_DIR}/include/ -DProtobuf_NO_SYSTEM_PATHS=True

make

else

cmake -DUSE_CLANG=OFF -DBOOST_ROOT=/disk/local/boost_1_50_0/ -DBoost_NO_SYSTEM_PATHS=True

fi
