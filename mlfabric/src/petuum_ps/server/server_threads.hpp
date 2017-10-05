// author: jinliang
#pragma once

#include <petuum_ps/server/server_thread_group.hpp>

namespace petuum {

  class ServerThreads {
  public:
    static void Init();
    static void ShutDown();

  private:
    static ServerThreadGroup *server_thread_group_;
  };

}
