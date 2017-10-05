#include <petuum_ps/replica/replica_threads.hpp>

namespace petuum {

  ReplicaThreadGroup *ReplicaThreads::replica_thread_group_;

  void ReplicaThreads::Init() {
    replica_thread_group_ = new ReplicaThreadGroup();
    replica_thread_group_->Start();
  }

  void ReplicaThreads::ShutDown() {
    replica_thread_group_->ShutDown();
    delete replica_thread_group_;
  }

}
