# Installation Guide
<!---
The Caffe app can be found in `bosen/app/caffe/`. From this point on, all instructions will assume you are in `bosen/app/caffe/`, and that you have **already completed the [[Petuum setup instructions|Installation]]**.
-->

## Pre-requisites
We have tested Poseidon on GCC v4.7, v4.8, CUDA v6.5 and v7.5, cuDNN v2 and v3. If you are having trouble compiling, please try using CUDA v7.5 and cuDNN v3.

**Note: Some users may encounter an issue involving the Boost library and the CUDA 7.0 compiler, which produces compile errors in .cuo files. We are working on fixing this issue.**

## Installation Procedures
Download Poseidon by running the following commands:

    git clone https://github.com/petuum/poseidon.git
    cd poseidon

Install third party pre-requisites - run the following commands under the poseidon directory:

    sudo apt-get -y install g++ make python-dev libxml2-dev libxslt-dev git zlibc zlib1g zlib1g-dev libbz2-1.0 libbz2-dev
    git clone https://github.com/petuum/third_party.git
    cd third_party
    make -j2
    cd ..


Next, compile the Bösen key-value store, which enables distributed multi-machine support in Caffe:

    cd ps
    make
    cd ..

Now, we are ready to compile Caffe. Install the [Caffe prerequisites](http://caffe.berkeleyvision.org/installation.html). If you are using Ubuntu 14.04, we have provided a script to do so:
    
    sh ../scripts/install-pmls-caffe-dependencies-ubuntu1404.sh

Enter `Y` when you are asked `Do you want to continue [Y/n]?`. 

After setting up the Caffe prerequisites, create the Caffe configuration file:

    cp Makefile.config.example Makefile.config

You may want to configure `Makefile.config` for your environment; see the instructions inside the file for details. By default, `Makefile.config` assumes that GPU support is available; CPU-only users will need to modify it. Finally, build Caffe using

    make all -j4

## Password-less SSH authentication

Petuum uses `ssh` (and `mpirun`, which invokes `ssh`) to coordinate tasks on different machines, **even if you are only using a single machine**. This requires password-less key-based authentication on all machines you are going to use (Petuum will fail if a password prompt appears).

If you don't already have an SSH key, generate one via

```
ssh-keygen
```

You'll then need to add your public key to each machine, by appending your public key file `~/.ssh/id_rsa.pub` to `~/.ssh/authorized_keys` on each machine. If your home directory is on a shared filesystem visible to all machines, then simply run

```
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
```

If the machines do not have a shared filesystem, you need to upload your public key to each machine, and the append it as described above.

**Note:** Password-less authentication can fail if `~/.ssh/authorized_keys` does not have the correct permissions. To fix this, run `chmod 600 ~/.ssh/authorized_keys`.

Now you are ready, you may refer to the [setup guide](distributed-guide.md) or the [performance report](performance.md) for the next step. 
