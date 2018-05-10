#!/usr/bin/env sh

# clone Poseidon
git clone https://github.com/petuum/poseidon.git
cd poseidon

# Install the dependencies
sudo apt-get -y install g++ make python-dev libxml2-dev libxslt-dev git zlibc zlib1g zlib1g-dev libbz2-1.0 libbz2-dev

# clone and compile the third party libs
git clone https://github.com/petuum/third_party.git
cd third_party
make -j2

# complie bosen
cd ..
cd ps
make

# setup the third party libs for Caffe
#cd ..
#sh script/setup_third_party.sh

# duplicate the Makefile.config
cp Makefile.config.example Makefile.config

# make Caffe
make all -j4
