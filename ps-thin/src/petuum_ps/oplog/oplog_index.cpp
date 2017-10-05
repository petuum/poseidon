
#include <petuum_ps/oplog/oplog_index.hpp>
#include <petuum_ps/thread/context.hpp>
#include <petuum_ps/include/constants.hpp>
#include <glog/logging.h>

namespace petuum {

PartitionOpLogIndex::PartitionOpLogIndex(size_t capacity)
    : capacity_(capacity), locks_(GlobalContext::GetLockPoolSize()),
      shared_oplog_index_(new cuckoohash_map<int32_t, bool>(
          static_cast<size_t>(capacity * kCuckooExpansionFactor))) {}

PartitionOpLogIndex::~PartitionOpLogIndex() { delete shared_oplog_index_; }

PartitionOpLogIndex::PartitionOpLogIndex(PartitionOpLogIndex &&other)
    : capacity_(other.capacity_),
      shared_oplog_index_(other.shared_oplog_index_) {
  other.shared_oplog_index_ = nullptr;
}

void
PartitionOpLogIndex::AddIndex(const std::unordered_set<int32_t> &oplog_index) {
  smtx_.lock_shared();
  for (int iter : oplog_index) {
    locks_.Lock(iter);
    shared_oplog_index_->insert(iter, true);
    locks_.Unlock(iter);
  }
  smtx_.unlock_shared();
}

cuckoohash_map<int32_t, bool> *PartitionOpLogIndex::Reset() {
  smtx_.lock();
  cuckoohash_map<int32_t, bool> *old_index = shared_oplog_index_;
  shared_oplog_index_ = new cuckoohash_map<int32_t, bool>(
      static_cast<size_t>(capacity_ * kCuckooExpansionFactor));
  smtx_.unlock();
  return old_index;
}

size_t PartitionOpLogIndex::GetNumRowOpLogs() {
  smtx_.lock();
  size_t num_row_oplogs = shared_oplog_index_->size();
  smtx_.unlock();
  return num_row_oplogs;
}

TableOpLogIndex::TableOpLogIndex(size_t capacity) {
  for (int32_t i = 0; i < GlobalContext::get_num_comm_channels_per_client();
       ++i) {
    partition_oplog_index_.emplace_back(capacity);
  }
}

void TableOpLogIndex::AddIndex(int32_t partition_num,
                               const std::unordered_set<int32_t> &oplog_index) {
  partition_oplog_index_[partition_num].AddIndex(oplog_index);
}

cuckoohash_map<int32_t, bool> *
TableOpLogIndex::ResetPartition(int32_t partition_num) {
  return partition_oplog_index_[partition_num].Reset();
}

size_t TableOpLogIndex::GetNumRowOpLogs(int32_t partition_num) {
  return partition_oplog_index_[partition_num].GetNumRowOpLogs();
}

/**
 * Add the row to the index indicating it has been modified.
 * NOTE: This function is not thread-safe.
 * However, it can be safely used with Caffe, since Caffe
 * uses a dedicated thread to call Inc / BatchInc / DenseInc on each table.
 * Thus, the function is guaranteed to be called from a single thread
 * exclusively.
 * @param row_id
 */
void TableOpLogIndex::AddRowIndex(int32_t row_id) {
  int partition_num = GlobalContext::GetPartitionCommChannelIndex(row_id);
  std::unordered_set<int32_t> row_set;
  row_set.insert(row_id);
  partition_oplog_index_[partition_num].AddIndex(row_set);
}
}
