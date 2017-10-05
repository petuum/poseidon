//
// Created by Raajay on 10/3/17.
//

#ifndef CAFFE_ONEDIMSTORAGE_HPP
#define CAFFE_ONEDIMSTORAGE_HPP

#include <cstdint>
#include <boost/unordered/unordered_map.hpp>

namespace petuum {

    template<class K1, class K2, class V>
    class TwoDimStorage {
    public:
        TwoDimStorage() = default;

        ~TwoDimStorage() = default;

        void Reset() {
            data_.clear();
        }

        V Get(K1 key1, K2 key2) const {
            auto iter1 = data_.find(key1);
            if (data_.end() == iter1) {
                return nullptr;
            }
            auto iter2 = iter1->second.find(key2);
            return (iter2 == iter1->second.end()) ? nullptr : iter2->second;
        }

        void Put(K1 key1, K2 key2, V value) {
            if(data_.end() == data_.find(key1)) {
                data_.insert(std::make_pair(key1, std::map<K2, V>()));
            }
            auto iter1 = data_.find(key1);
            CHECK_EQ(iter1 == data_.find(key1), false);

            auto iter2 = iter1->second.find(key2);
            CHECK_EQ(iter == data_.end(), true) << "Value " << v << " for key pair " << key1 << " " << key2 << " already exists";
            iter1->second.insert({key2, value});
        }

    private:
        std::map<K1, std::map<K2, V>> data_;
};
}

#endif //CAFFE_ONEDIMSTORAGE_HPP

