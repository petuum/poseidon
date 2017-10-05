#include <petuum_ps/aggregator/aggregator_threads.hpp>

namespace petuum {

  AggregatorThreadGroup *AggregatorThreads::aggregator_thread_group_;

  void AggregatorThreads::Init() {
    aggregator_thread_group_ = new AggregatorThreadGroup();
    aggregator_thread_group_->Start();
    VLOG(1) << "Initialize aggregator threads.";
  }

  void AggregatorThreads::ShutDown() {
    aggregator_thread_group_->ShutDown();
    delete aggregator_thread_group_;
  }
}
