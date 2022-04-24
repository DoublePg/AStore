# Code for AStore System

# Hardware Dependencies 
    CPU: Intel(R) Xeon(R) Silver 4216 CPU @ 2.10GHz
    RDMA: Mellanox Technologies MT27800 Family [ConnectX-5]
    
# Software Dependencies
    Ubuntu 18.04 
    cmake 3.10.2
    g++ 7.5.0

# How to Access
You may use Git to clone the repository from GitHub 

    git clone https://github.com/DoublePg/AStore.git

# Build
The path to the dataset can be modified by changing the *data_file* field in *Client.cc* and *server.cc*.
The corresponding workload can be modified in the *ssched.spawn* function in the *Client.cc*.
### Launch AStore server
    cd AStore/
    cmake .
    make -j12
    ./ycsb_server
### Launch AStore clients
    cd AStore/
    cmake .
    make -j12
    ./ycsb_client

# Datesets
The path to the dataset can be modified by changing the data_file field in Client.cc and server.cc.

    [Lognormal(190M 8-byte signed ints)](https://drive.google.com/file/d/1y-UBf8CuuFgAZkUg_2b_G8zh4iF_N-mq/view?usp=sharing)
    [YCSB(200M 8-byte unsigned ints)](https://drive.google.com/file/d/1Q89-v4FJLEwIKL3YY3oCeOEs0VUuv5bD/view?usp=sharing)
    [OpenStreetMap dataset].(https://registry.opendata.aws/osm)

