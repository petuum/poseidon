//
// Created by raajay on 10/3/17.
//

#ifndef CAFFE_TWODIMCOUNTER_HPP
#define CAFFE_TWODIMCOUNTER_HPP

#include <map>
#include <glog/logging.h>
namespace petuum {

template <class K1, class K2, class V> class TwoDimCounter {
public:
  TwoDimCounter() = default;

  ~TwoDimCounter() = default;

  void Reset() {
    for (auto &iter1 : data_) {
      iter1.second.clear();
    }
    data_.clear();
  }

  V Get(K1 key1, K2 key2) const {
    auto iter1 = data_.find(key1);
    if (data_.end() == iter1) {
      return 0;
    }
    auto iter2 = iter1->second.find(key2);
    return (iter1->second.end() == iter2) ? 0 : iter2->second;
  }

  V Get(K1 key1) const {
    auto iter1 = data_.find(key1);
    if (data_.end() == iter1) {
      return 0;
    }
    V return_value = 0;
    for (auto &iter2 : iter1->second) {
      return_value += iter2.second;
    }
    return return_value;
  }

  V GetAll() const {
    V return_value = 0;
    for (auto &iter1 : data_) {
      for (auto &iter2 : iter1.second) {
        return_value += iter2.second;
      }
    }
    return return_value;
  }

  void Increment(K1 key1, K2 key2, V value) {
    if (data_.end() == data_.find(key1)) {
      data_.insert(std::make_pair(key1, std::map<K2, V>()));
    }
    auto iter1 = data_.find(key1);
    CHECK_EQ(iter1 == data_.end(), false);

    auto iter2 = iter1->second.find(key2);
    if (iter1->second.end() == iter2) {
      iter1->second.insert({key2, value});
    } else {
      iter1->second[key2] += value;
    }
  }

  std::vector<K2> GetKeysPosValue(K1 key1) const {
    std::vector<K2> return_value;
    auto iter1 = data_.find(key1);

    if (iter1 == data_.end()) {
      return return_value;
    }

    for (auto &it2 : iter1->second) {
      if (it2.second > 0) {
        return_value.push_back(it2.first);
      }
    }
    return return_value;
  }

  std::vector<K1> GetDim1Keys() const {
    std::vector<K1> return_value;
    for (auto &it : data_) {
      return_value.push_back(it.first);
    }
    return return_value;
  }

  std::string ToString() {
      std::stringstream ss;
      for(auto &iter1 : data_) {
          for(auto &iter2 : iter1.second) {
              ss << "(" << iter1.first << ", " << iter2.first
                 << ", " << iter2.second << ")   ";
          }
      }
      return ss.str();
  }

private:
  std::map<K1, std::map<K2, V>> data_;
};
}

#endif // CAFFE_TWODIMCOUNTER_HPP
