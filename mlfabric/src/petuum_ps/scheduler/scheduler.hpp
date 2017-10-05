#pragma once

#include <petuum_ps/scheduler/scheduler_thread.hpp>
#include <pthread.h>
#include <petuum_ps/util/pthread_barrier.hpp>

namespace petuum {
  class Scheduler {
  public:
    static void Init();
    static void ShutDown();
  private:
    static SchedulerThread *scheduler_thread_;
    static pthread_barrier_t init_barrier_;
  };
}
