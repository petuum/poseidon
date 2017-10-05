#include <petuum_ps/replica/replica_thread_group.hpp>
#include <petuum_ps/replica/replica_thread.hpp>
#include <petuum_ps/thread/context.hpp>

namespace petuum {
  ReplicaThreadGroup::ReplicaThreadGroup():
    replica_thread_vec_(GlobalContext::get_num_comm_channels_per_client()) {

    // we create barrier for n+1 threads (n=number of replica threads we want to start).
    // Thus, the next pthread_barrier wait will synchronize the current thread with all
    // the replica threads.
    pthread_barrier_init(&init_barrier_, NULL,
                         GlobalContext::get_num_comm_channels_per_client() + 1);

    switch(GlobalContext::get_consistency_model()) {
    case SSP:
      {
        int idx = 0;
        int32_t client_id = GlobalContext::get_client_id();
        for (auto &replica_thread : replica_thread_vec_) {
          int32_t replica_thread_id = GlobalContext::get_replica_thread_id(client_id, idx);
          replica_thread = new ReplicaThread(replica_thread_id, &init_barrier_);
          //replica_thread = new ReplicaThread(
          //    GlobalContext::get_replica_thread_id(
          //        GlobalContext::get_client_id(), idx), &init_barrier_);
          ++idx;
        }
        VLOG(5) << "Created " << idx << " ReplicaThread instances";
      }
      break;
    default:
      LOG(FATAL) << "Unknown consistency model "
                 << GlobalContext::get_consistency_model();
    }
  }

  ReplicaThreadGroup::~ReplicaThreadGroup() {
    for (auto &replica_thread : replica_thread_vec_) {
      delete replica_thread;
    }
  }

  void ReplicaThreadGroup::Start() {
    for (const auto &replica_thread : replica_thread_vec_) {
      replica_thread->Start();
    }

    pthread_barrier_wait(&init_barrier_);
  }

  void ReplicaThreadGroup::ShutDown() {
    for (const auto &replica_thread : replica_thread_vec_) {
      replica_thread->ShutDown();
    }
  }

} // end namespace -- petuum
