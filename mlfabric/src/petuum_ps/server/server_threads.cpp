#include <petuum_ps/server/server_threads.hpp>

namespace petuum {

  ServerThreadGroup *ServerThreads::server_thread_group_;

  void ServerThreads::Init() {
    server_thread_group_ = new ServerThreadGroup();
    server_thread_group_->Start();
  }

  void ServerThreads::ShutDown() {
    server_thread_group_->ShutDown();
    delete server_thread_group_;
  }

}
