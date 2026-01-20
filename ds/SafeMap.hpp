#pragma once

#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <shared_mutex>

template<typename K, typename V>
class SafeMap {
    public:
    SafeMap() = default;

    V get(const K& key) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = map_.find(key);
        if(it == map_.end()) {
            throw std::out_of_range("SafeMap: Key not found.");
        } 
        
        return it->second;
    }

    bool contains(const K& key) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return map_.find(key) != map_.end();
    }

    bool remove(const K& key) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        return map_.erase(key) > 0;
    }

    std::optional<V> put(const K& key, const V& value) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        std::optional<V> ret = std::nullopt;
        if(map_.find(key) != map_.end()) {
            ret = map_[key];
        }
        map_[key] = value;
        return ret;
    }

    size_t size() const { 
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return map_.size();
    }

    template<typename Func>
    bool update(const K& key, const V& expected, Func updater) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        auto it = map_.find(key);

        if(it != map_.end() && it->second == expected) {
            updater(it->second);
            return true;
        }

        return false;
    }

    // Adhoc functions, specifically meant for updating _datafiles
    size_t add_to_end(const V& value, int ind = 0) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        size_t new_index = map_.size() - ind;
        map_[new_index] = value;
        return new_index;
    }

    std::vector<std::pair<K, V>> items() {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        std::vector<std::pair<K, V>> ret;
        for(const auto& [k, v] : map_) {
            ret.emplace_back(k, v);
        }
        return ret;
    }

    void clear() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        map_.clear();
    }

    private:
    std::unordered_map<K, V> map_;
    mutable std::shared_mutex mutex_;
};