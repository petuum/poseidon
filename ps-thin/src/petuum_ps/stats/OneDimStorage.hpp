//
// Created by Raajay on 10/3/17.
//

#ifndef CAFFE_ONEDIMSTORAGE_HPP
#define CAFFE_ONEDIMSTORAGE_HPP

#include <cstdint>
#include <boost/unordered/unordered_map.hpp>

namespace petuum {

    template<class K, class V>
    class OneDimStorage {
    public:
        OneDimStorage() = default;

        ~OneDimStorage() = default;

        void Reset() {
            data_.clear();
        }

        V Get(K key) const {
            auto iter = data_.find(key);
            return (iter == data_.end()) ? nullptr : iter->second;
        }

        void Put(K key, V value) {
            auto iter = data_.find(key);
            CHECK_EQ(iter == data_.end(), true) << "Value " << value << " for key " << key << " already exists";
            data_[key] = value;
        }

    private:
        boost::unordered::unordered_map<K, V> data_;
};
}

#endif //CAFFE_ONEDIMSTORAGE_HPP
