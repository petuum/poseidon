//
// Created by Raajay on 10/2/17.
//

#ifndef CAFFE_BYTECOUNTER_HPP
#define CAFFE_BYTECOUNTER_HPP

#include <cstdint>
#include <boost/unordered/unordered_map.hpp>

namespace petuum {

template <class K, class V> class OneDimCounter {
public:
  /**
   * Constructor
   */
  OneDimCounter() = default;

  /**
   * Destructor
   */
  ~OneDimCounter() = default;

  /**
   * Set values of all keys to zero
   */
  void Reset() { data_.clear(); }

  /**
   * Get counter value or a key
   */
  V Get(K key) {
    auto iter = data_.find(key);
    return (iter == data_.end()) ? 0 : iter->second;
  }

  /**
   * Get cumulative counter
   */
  V GetAll() {
    V return_val = 0;
    for (auto &it : data_) {
      return_val += it.second;
    }
    return return_val;
  }

  /**
   * Increment counter
   */
  void Increment(K key, V value) {
    auto iter = data_.find(key);
    if (data_.end() == iter) {
      data_.emplace(key, value);
    } else {
      data_[key] += value;
    }
  }

  /**
   * Return keys with pos values
   */
  const std::vector<K> GetKeysPosValue() {
    std::vector<K> return_value;
    for (auto &it : data_) {
      if (it.second > 0) {
        return_value.push_back(it.first);
      }
    }
    return return_value;
  }

  std::string ToString() {
      std::stringstream ss;
      for(auto &it : data_) {
          ss << "(" << it.first << "," << it.second << ")  ";
      }
      return ss.str();
  }

private:
  boost::unordered::unordered_map<K, V> data_;
};
}

#endif // CAFFE_BYTECOUNTER_HPP
