// author: raajay

#pragma once

#include <petuum_ps/aggregator/aggregator_thread_group.hpp>

namespace petuum {

class AggregatorThreads {
public:
  static void Start();
  static void Init();
  static void ShutDown();
  static void AppThreadRegister();
  static void AppThreadDeregister();

private:
  static AggregatorThreadGroup *aggregator_thread_group_;
};

}
