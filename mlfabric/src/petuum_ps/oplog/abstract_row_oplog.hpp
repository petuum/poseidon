// Author: jinliang
#pragma once

#include <stdint.h>
#include <map>
#include <algorithm>

#include <functional>
#include <boost/noncopyable.hpp>

namespace petuum {

  typedef std::function<void(int32_t, void *)> InitUpdateFunc;
  typedef std::function<bool(const void*)> CheckZeroUpdateFunc;

  class AbstractRowOpLog : boost::noncopyable {
  public:
    AbstractRowOpLog(size_t update_size):
      update_size_(update_size),
      global_version_(-1) { } // set default version as -1

    virtual ~AbstractRowOpLog() { }

    // clean up the oplogs and allow the RowOpLog to be reused as a fresh one
    virtual void Reset() = 0;

    virtual void* Find(int32_t col_id) = 0;

    virtual const void* FindConst(int32_t col_id) const = 0;

    virtual void* FindCreate(int32_t col_id) = 0;

    // Guaranteed ordered traversal
    virtual void* BeginIterate(int32_t *column_id) = 0;

    virtual void* Next(int32_t *column_id) = 0;

    // Guaranteed ordered traversal, in ascending order of column_id
    virtual const void* BeginIterateConst(int32_t *column_id) const = 0;

    virtual const void* NextConst(int32_t *column_id) const = 0;

    virtual size_t GetSize() const = 0;
    virtual size_t ClearZerosAndGetNoneZeroSize() = 0;

    virtual size_t GetSparseSerializedSize() = 0;
    virtual size_t GetDenseSerializedSize() = 0;

    virtual size_t SerializeSparse(void *mem) = 0;
    virtual size_t SerializeDense(void *mem) = 0;

    /**
     * Set the version of the model used to compute the oplog. If there are
     * multiple versions of the model used, then set the version as the least
     * one.
     */
    virtual void SetGlobalVersion(int32_t global_version) {
      if(global_version_ == -1) {
        // if version is default (not been previously set) use the value
        global_version_ = global_version;
      } else {
        // if already set, use the minimum value
        global_version_ = std::min(global_version, global_version_);
      }
    }

    virtual int32_t GetGlobalVersion() {
      return global_version_;
    }

    // Certain oplog types do not support dense serialization.
    virtual const void *ParseDenseSerializedOpLog(const void *mem,
                                                  int32_t *num_updates,
                                                  size_t *serialized_size) const = 0;

    virtual void OverwriteWithDenseUpdate(const void *updates,
                                          int32_t index_st,
                                          int32_t num_updates) = 0;

    // The default sparse serialization format:
    // 1) number of updates in that row
    // 2) total size for column ids
    // 3) total size for update array
    virtual const void *ParseSparseSerializedOpLog(const void *mem,
                                                   int32_t const ** column_ids,
                                                   int32_t *num_updates,
                                                   size_t *serialized_size) const {

      const uint8_t *mem_uint8 = reinterpret_cast<const uint8_t*>(mem);
      *num_updates = *(reinterpret_cast<const int32_t*>(mem_uint8));

      mem_uint8 += sizeof(int32_t);
      *column_ids = reinterpret_cast<const int32_t*>(mem_uint8);

      mem_uint8 += *num_updates*sizeof(int32_t);

      *serialized_size = sizeof(int32_t) + *num_updates*(sizeof(int32_t) + update_size_);

      return mem_uint8;

    } // end function -- Parse serialized op log

    typedef size_t (AbstractRowOpLog::*SerializeFunc)(void *mem);

  protected:
    size_t update_size_;
    int32_t global_version_;

  }; // end class - abstract op log

} // end namespace -- petuum
