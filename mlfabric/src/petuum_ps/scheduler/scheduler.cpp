#include <petuum_ps/scheduler/scheduler.hpp>

namespace petuum {
    // declare the static variables again in this file
    SchedulerThread *Scheduler::scheduler_thread_;
    pthread_barrier_t Scheduler::init_barrier_;

    void Scheduler::Init() {
      // we set up a barrier, that use to synchronize the current thread,
      // and the new scheduler thread that will be spawned when we Start and
      // scheduler thread object.
      pthread_barrier_init(&init_barrier_, NULL, 2);
      scheduler_thread_ = new SchedulerThread(&init_barrier_);
      scheduler_thread_->Start();
      // wait until the comm bus has been setup for the spawned thread. Comm
      // bus setup, would have been triggered upon Start.
      pthread_barrier_wait(&init_barrier_);
    }

    void Scheduler::ShutDown() {
        scheduler_thread_->ShutDown();
        delete scheduler_thread_;
    }
} // end namespace -- petuum
