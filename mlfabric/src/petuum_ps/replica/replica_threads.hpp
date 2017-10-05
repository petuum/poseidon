// author: raajay
#pragma once

#include <petuum_ps/replica/replica_thread_group.hpp>

namespace petuum {

  class ReplicaThreads {
  public:
    static void Init();
    static void ShutDown();

  private:
    static ReplicaThreadGroup *replica_thread_group_;
  };

}
