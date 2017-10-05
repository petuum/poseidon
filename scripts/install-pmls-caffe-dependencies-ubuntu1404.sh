#!/bin/bash

# thirdparty deps
sudo apt-get install -y g++ make python-dev libxml2-dev libxslt-dev git zlibc zlib1g zlib1g-dev libbz2-1.0 libbz2-dev
sudo apt-get install -y libatlas-base-dev libprotobuf-dev libsnappy-dev libopencv-dev libhdf5-serial-dev

# caffe deps
sudo apt-get install -y libatlas-base-dev libprotobuf-dev libsnappy-dev libopencv-dev libhdf5-serial-dev
sudo apt-get install -y libnuma-dev uuid-dev

sudo apt-get -y remove cmake
sudo apt-get -y install software-properties-common
sudo add-apt-repository ppa:george-edison55/cmake-3.x
sudo apt-get update
sudo apt-get -y install cmake
## deps for CMake
sudo apt-get install -y libgoogle-glog-dev \
    libyaml-cpp-dev \
    libgoogle-perftools-dev \
    libsnappy-dev \
    libsparsehash-dev \
    libgflags-dev \
    libboost-filesystem-dev \
    libboost-system-dev \
    libboost-thread-dev \
    libleveldb-dev \
    libconfig++-dev \
    libghc-hashtables-dev \
    libtcmalloc-minimal4 \
    libevent-pthreads-2.0-5 \
    libeigen3-dev \
    liblmdb-dev

## multi-threaded CPU support
sudo apt-get install -y libopenblas-dev

sudo apt-get install -y libtiff4-dev

# install latest zeromq from source

TEMP_DIR=$(mktemp -d)

function install_zeromq() {
    ZMQ_DOWNLOAD_URL="https://github.com/zeromq/libzmq/releases/download/v4.2.2/zeromq-4.2.2.tar.gz"
    TAR_FILE_NAME=$(basename ${ZMQ_DOWNLOAD_URL})
    BASE_NAME=$(basename ${ZMQ_DOWNLOAD_URL} .tar.gz)
    echo $TAR_FILE_NAME
    echo $BASE_NAME

    # pull the file and extract it
    cd $TEMP_DIR
    wget -O ${TEMP_DIR}/${TAR_FILE_NAME} ${ZMQ_DOWNLOAD_URL} -o ${TEMP_DIR}/zeromq.wget.log
    tar -x -v -z -f ${TAR_FILE_NAME} > /dev/null

    ZMQ_INSTALL_DIR=${TEMP_DIR}/${BASE_NAME}
    echo "Installing ZMQ from ${ZMQ_INSTALL_DIR}"

    cd $ZMQ_INSTALL_DIR
    ./configure
    make -j20
    sudo make install
}


function install_cppzmq() {
    git clone https://github.com/zeromq/cppzmq.git ${TEMP_DIR}/cppzmq
    cd ${TEMP_DIR}/cppzmq
    mkdir build && cd build
    cmake ../
    sudo make -j10 install
}


#install_zeromq
install_cppzmq

# install tools
sudo apt-get install -y clang-format-3.5
sudo apt-get install -y libgtest-dev
