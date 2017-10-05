#include <petuum_ps/aggregator/aggregator_thread_group.hpp>
#include <petuum_ps/aggregator/aggregator_thread.hpp>
#include <petuum_ps/thread/context.hpp>

namespace petuum {

  AggregatorThreadGroup::AggregatorThreadGroup() :
    aggregator_thread_vec_(GlobalContext::get_num_comm_channels_per_client()) {

    pthread_barrier_init(&init_barrier_, NULL,
                         GlobalContext::get_num_comm_channels_per_client() + 1);

    int idx = 0;
    int32_t client_id = GlobalContext::get_client_id();
    for (auto &aggregator_thread : aggregator_thread_vec_) {
      int32_t aggregator_thread_id = GlobalContext::get_aggregator_thread_id(client_id, idx);
      aggregator_thread = new AggregatorThread(aggregator_thread_id, idx, &init_barrier_);
      ++idx;
    }
    VLOG(5) << "Created " << idx << " AggregatorThread instances";
  }



  AggregatorThreadGroup::~AggregatorThreadGroup() {
    for(auto &aggregator_thread : aggregator_thread_vec_) {
      delete aggregator_thread;
    }
  }

  void AggregatorThreadGroup::Start() {
    for (const auto &aggregator_thread : aggregator_thread_vec_) {
      aggregator_thread->Start();
    }
    pthread_barrier_wait(&init_barrier_);
  }


  void AggregatorThreadGroup::ShutDown() {
    for( const auto &aggregator_thread : aggregator_thread_vec_) {
      aggregator_thread->ShutDown();
    }
  }
}
