// author: jinliang
#pragma once

#include <petuum_ps/server/server_thread.hpp>

namespace petuum {
  class ServerThreadGroup {
  public:
    ServerThreadGroup();
    ~ServerThreadGroup();

    void Start();
    void ShutDown();
  private:
    std::vector<ServerThread*> server_thread_vec_;
    pthread_barrier_t init_barrier_;
  };

} // end namespace -- petuum
