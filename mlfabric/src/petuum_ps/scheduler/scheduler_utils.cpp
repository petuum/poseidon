#include <petuum_ps/scheduler/scheduler_utils.hpp>


namespace petuum {
  StoredValue::StoredValue(int32_t bg_id, int32_t unique_id, int32_t server_id) {
    bg_id_ = bg_id;
    unique_id_ = unique_id;
    destination_server_id_ = server_id;
  }



  bool AggregateComparator::operator()(ClientRequestAggregate* a, ClientRequestAggregate* b) {
    return (a->GetVersion() < b->GetVersion());
  }


  ClientRequestAggregate::ClientRequestAggregate(int32_t worker_client_id, int32_t server_client_id):
    worker_client_id_(worker_client_id),
    server_client_id_(server_client_id),
    total_size_(0),
    min_version_(INT_MAX) {}

  /**
     Copy constructor.
  */
  ClientRequestAggregate::ClientRequestAggregate(ClientRequestAggregate* other) {
    worker_client_id_ = other->worker_client_id_;
    server_client_id_ = other->server_client_id_;
    for(uint i = 0; i < other->Count(); i++) {
      bg_ids_.push_back(other->bg_ids_[i]);
      server_ids_.push_back(other->server_ids_[i]);
      bg_unique_ids_.push_back(other->bg_unique_ids_[i]);
      client_version_ids_.push_back(other->client_version_ids_[i]);
      gradient_sizes_.push_back(other->gradient_sizes_[i]);
    }
    total_size_ = other->total_size_;
    min_version_ = other->min_version_;
  }

  ClientRequestAggregate::~ClientRequestAggregate() {}

  void ClientRequestAggregate::AddRequest(int32_t bg_id,
                                          int32_t server_id,
                                          int32_t gradient_size,
                                          int32_t client_version_id,
                                          int32_t bg_unique_id) {
    bg_ids_.push_back(bg_id);
    server_ids_.push_back(server_id);
    gradient_sizes_.push_back(gradient_size);
    bg_unique_ids_.push_back(bg_unique_id);
    client_version_ids_.push_back(client_version_id);
    total_size_ += gradient_size;
    min_version_ = std::min(min_version_, client_version_id);
    // CHECK_LE(this->Count(), GlobalContext::get_num_comm_channels_per_client())
    //   << "number of requests greater than #comm channels";
  }

  void ClientRequestAggregate::Reset() {
    bg_ids_.clear();
    server_ids_.clear();
    bg_unique_ids_.clear();
    client_version_ids_.clear();
    gradient_sizes_.clear();
    total_size_ = 0;
    min_version_ = INT_MAX;
  }

  int32_t ClientRequestAggregate::Count() {
    return bg_ids_.size();
  }

  int32_t ClientRequestAggregate::GetWorkerID(int index) {
    CHECK_LT(index, Count()) << "index exceeds number of requests in aggregate";
    return bg_ids_[index];
  }

  int32_t ClientRequestAggregate::GetServerID(int index) {
    CHECK_LT(index, Count()) << "index exceeds number of requests in aggregate";
    return server_ids_[index];
  }

  int32_t ClientRequestAggregate::GetUniqueID(int index) {
    CHECK_LT(index, Count()) << "index exceeds number of requests in aggregate";
    return bg_unique_ids_[index];
  }

  int32_t ClientRequestAggregate::GetVersion() {
    return min_version_;
  }

  int32_t ClientRequestAggregate::GetSize() {
    return total_size_;
  }

  std::string ClientRequestAggregate::toString() {
    std::stringstream ss;
    ss << "version=" << GetVersion() << " size=" << GetSize();
    for(uint i = 0; i < Count(); i++) {
      ss << " (" << GetWorkerID(i) << ", " << GetServerID(i) << ", " << GetUniqueID(i)  <<  "),";
    }
    return ss.str();
  }
}
