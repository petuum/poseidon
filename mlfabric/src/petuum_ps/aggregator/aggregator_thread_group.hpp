// author: raajay
#pragma once

#include <pthread.h>
#include <petuum_ps/util/pthread_barrier.hpp>
#include <petuum_ps/aggregator/aggregator_thread.hpp>

namespace petuum {

  class AggregatorThreadGroup {
  public:
    AggregatorThreadGroup();
    ~AggregatorThreadGroup();

    void Start();
    void ShutDown();

  private:
    std::vector<AggregatorThread*> aggregator_thread_vec_;
    pthread_barrier_t init_barrier_;
    int32_t aggregator_id_st_; // TODO do we need this
  };

}
