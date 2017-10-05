#pragma once

#include <boost/noncopyable.hpp>

namespace petuum {
class ServerOpLogSerializer : boost::noncopyable {
public:
  ServerOpLogSerializer() : mem_(nullptr), num_tables_(0) {}

  ~ServerOpLogSerializer() = default;

  size_t Init(int32_t server_id, const TwoDimCounter<int32_t, int32_t, size_t> &server_table_byte_counter) {

    const std::vector<int32_t> non_empty_tables = server_table_byte_counter.GetKeysPosValue(server_id);
    num_tables_ = (int32_t)non_empty_tables.size();

    if (0 == num_tables_) {
      return 0;
    }

    // 4 bytes for storing the number of tables
    size_t total_size = sizeof(int32_t);

    for (auto table_id : non_empty_tables) {
      // store the offset for table_id
      offset_map_[table_id] = total_size;
      // Increment the offset by space occupied by current table. It includes:
      // 1) sizeof(int32_t) bytes for table_id,
      // 2) sizeof(size_t) bytes for update size (size of row) in this table
      // 3) Sum of all row oplogs + num rows from current table to server
      total_size += sizeof(int32_t) + sizeof(size_t) +
                    server_table_byte_counter.Get(server_id, table_id);
    }
    return total_size;
  }

  // does not take ownership
  void AssignMem(void *mem, size_t &bytes_written) {
    mem_ = reinterpret_cast<uint8_t *>(mem);
    *(reinterpret_cast<int32_t *>(mem_)) = num_tables_;
    bytes_written += sizeof(int32_t);
  }

  /**
   * Return the offset into the memory where all table_id relevant data will be
   * stored.
   * @param table_id
   * @return
   */
  void *GetTablePtr(int32_t table_id) {
    CHECK_EQ(mem_ == nullptr, false);

    auto iter = offset_map_.find(table_id);
    if (iter == offset_map_.end()) {
      return nullptr;
    }
    return mem_ + iter->second;
  }

  std::string stringifyTableOffsets() {
      std::stringstream ss;
      return ss.str();
  }

private:
  std::map<int32_t, size_t> offset_map_;
  uint8_t *mem_;
  int32_t num_tables_;
};
}
