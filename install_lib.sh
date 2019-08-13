sudo rm -r /usr/include/rocksdb
sudo cp -r include/rocksdb /usr/include
sudo rm /usr/lib/librocksdb.*
sudo cp librocksdb.a librocksdb.so.5.18.3 /usr/lib
sudo ln -s /usr/lib/librocksdb.so.5.18.3 /usr/lib/librocksdb.so.5.18
sudo ln -s /usr/lib/librocksdb.so.5.18.3 /usr/lib/librocksdb.so.5
sudo ln -s /usr/lib/librocksdb.so.5.18.3 /usr/lib/librocksdb.so

sudo ldconfig
