// author: raajay
#pragma once

#include <petuum_ps/replica/replica_thread.hpp>

namespace petuum {
  class ReplicaThreadGroup {
  public:
    ReplicaThreadGroup();
    ~ReplicaThreadGroup();

    void Start();
    void ShutDown();
  private:
    std::vector<ReplicaThread*> replica_thread_vec_;
    pthread_barrier_t init_barrier_;
  };

} // end namespace -- petuum
