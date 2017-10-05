#include <petuum_ps/server/server_thread_group.hpp>
#include <petuum_ps/server/server_thread.hpp>
#include <petuum_ps/thread/context.hpp>

namespace petuum {
  ServerThreadGroup::ServerThreadGroup():
    server_thread_vec_(GlobalContext::get_num_comm_channels_per_client()) {

    // we create barrier for n+1 threads (n=number of server threads we want to start).
    // Thus, the next pthread_barrier wait will synchronize the current thread with all
    // the server threads.
    pthread_barrier_init(&init_barrier_, NULL,
                         GlobalContext::get_num_comm_channels_per_client() + 1);

    switch(GlobalContext::get_consistency_model()) {
    case SSP:
      {
        int idx = 0;
        int32_t client_id = GlobalContext::get_client_id();
        for (auto &server_thread : server_thread_vec_) {
          int32_t server_thread_id = GlobalContext::get_server_thread_id(client_id, idx);
          server_thread = new ServerThread(server_thread_id, &init_barrier_);
          //server_thread = new ServerThread(
          //    GlobalContext::get_server_thread_id(
          //        GlobalContext::get_client_id(), idx), &init_barrier_);
          ++idx;
        }
        VLOG(5) << "Created " << idx << " ServerThread instances";
      }
      break;
    default:
      LOG(FATAL) << "Unknown consistency model "
                 << GlobalContext::get_consistency_model();
    }
  }

  ServerThreadGroup::~ServerThreadGroup() {
    for (auto &server_thread : server_thread_vec_) {
      delete server_thread;
    }
  }

  void ServerThreadGroup::Start() {
    for (const auto &server_thread : server_thread_vec_) {
      server_thread->Start();
    }

    pthread_barrier_wait(&init_barrier_);
  }

  void ServerThreadGroup::ShutDown() {
    for (const auto &server_thread : server_thread_vec_) {
      server_thread->ShutDown();
    }
  }

} // end namespace -- petuum
