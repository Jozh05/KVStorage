#pragma once
#include <tuple>
#include <cstdint>
#include <string>
#include <optional>
#include <vector>
#include <unordered_map>
#include <map>
#include <span>
#include <chrono>
#include <atomic>
#include <shared_mutex>
#include <mutex>


template<typename Clock>
class KVStorage {

public:
    
    KVStorage(const KVStorage&)            = delete;
    KVStorage& operator=(const KVStorage&) = delete;
    KVStorage(KVStorage&&)                 = delete;
    KVStorage& operator=(KVStorage&&)      = delete;

    explicit KVStorage(std::span<std::tuple<std::string /*key*/, std::string /*value*/,
    uint32_t /*ttl*/>> entries, Clock clock = Clock()) {
        hashIndex.reserve(entries.size());
        for (const auto& [key, value, ttl] : entries) {
            set(std::move(key), std::move(value), ttl);
        }
    }
    
    ~KVStorage() = default;

    void set(std::string key, std::string value, uint32_t ttl) {
        auto lock = writer_lock();

        const time_point expire_at = compute_expire_time(ttl);

        if (auto hash_it = hashIndex.find(std::string_view(key)); hash_it != hashIndex.end()) {
            update_value(hash_it->second, std::move(value), expire_at);
        } else {
            MapIt it = insert_new(std::move(key), std::move(value));
            if (expire_at != time_point::max()) {
                link_expiry(it->second, it, expire_at);
            }
        }
    }

    bool remove(std::string_view key) {
        auto lock = writer_lock();

        auto hash_it = hashIndex.find(key);
        if (hash_it == hashIndex.end()) return false;

        MapIt it = hash_it->second;
        Entry& entry = it->second;
        if (entry.expiry_it != expiryIndex.end()) {
            expiryIndex.erase(entry.expiry_it);
        }
        hashIndex.erase(hash_it);
        sorted.erase(it);
        return true;
    }

    std::optional<std::string> get(std::string_view key) const {

        reader_gate();
        while (true) {
            std::shared_lock<std::shared_mutex> lock(mtx);
            if (writers_count.load(std::memory_order_acquire) == 0) {
                auto hash_it = hashIndex.find(key);
                
                if (hash_it == hashIndex.end())
                    return std::nullopt;
                
                typename SortedMap::const_iterator const_it = hash_it->second;
                const Entry& entry = const_it->second;
                if (is_expired_now(entry))
                    return std::nullopt;
                return entry.value;
            }
            lock.unlock();
            reader_gate();
        }

        return std::nullopt;
    }

    std::vector<std::pair<std::string, std::string>> getManySorted(std::string_view key,
    uint32_t count) const {
        reader_gate();

        while (true) {
            std::shared_lock<std::shared_mutex> lock(mtx);
            if (writers_count.load(std::memory_order_acquire) == 0) {
                std::vector<std::pair<std::string, std::string>> result;
                result.reserve(count);

                auto it = sorted.lower_bound(key);
                for (;it != sorted.end() && result.size() < count; ++it) {
                    const Entry& entry = it->second;
                    if (!is_expired_now(entry)) {
                        result.emplace_back(it->first, entry.value);
                    }
                }
                return result;
            }
            lock.unlock();
            reader_gate();
        }
    }

    std::optional<std::pair<std::string, std::string>> removeOneExpiredEntry() {
        auto lock = writer_lock();

        const auto now = Clock::now();
        auto exp_it = expiryIndex.begin();
        if (exp_it == expiryIndex.end() || exp_it->first > now) 
            return std::nullopt;
        
        auto hash_it = hashIndex.find(exp_it->second);
        if (hash_it == hashIndex.end()) {
            expiryIndex.erase(exp_it);
            return std::nullopt;
        }

        MapIt it = hash_it->second;
        std::string value = std::move(it->second.value);
        std::string key = std::move(it->first);

        expiryIndex.erase(exp_it);
        hashIndex.erase(hash_it);
        sorted.erase(it);

        return std::make_pair(std::move(key), std::move(value));
    }



private:
    using time_point = typename Clock::time_point;

    using ExpiryIndex = std::multimap<time_point, std::string_view>;
    using ExpIt = ExpiryIndex::iterator;

    struct Entry {
        std::string value;
        ExpIt expiry_it;
    };

    using SortedMap = std::map<std::string, Entry, std::less<>>;
    using MapIt = SortedMap::iterator;

    using Hash = std::hash<std::string_view>;
    using Eq = std::equal_to<std::string_view>;
    using HashIndex = std::unordered_map<std::string_view, MapIt, Hash, Eq>;

    
    HashIndex hashIndex;
    SortedMap sorted;
    ExpiryIndex expiryIndex;

    mutable std::shared_mutex mtx;
    mutable std::atomic_uint32_t writers_count = 0;




private:
    bool is_expired_now(const Entry& e) const {
        if (e.expiry_it == expiryIndex.end()) 
            return false;
        return e.expiry_it->first <= Clock::now();
    }

    void reader_gate() const {
        uint32_t cnt = writers_count.load(std::memory_order_acquire);
        while (cnt != 0) {
            writers_count.wait(cnt, std::memory_order_acquire);
            cnt = writers_count.load(std::memory_order_acquire);
        }
    }

    time_point compute_expire_time(uint32_t ttl) const noexcept {
        if (ttl == 0) return time_point::max();
        using seconds = std::chrono::seconds;
        return Clock::now() + seconds(ttl);
    }

    void update_value(MapIt it, std::string&& new_value, time_point expire_at) {
        Entry& entry = it->second;
        entry.value = std::move(new_value);
        unlink_expiry(entry);
        if (expire_at != time_point::max()) {
            link_expiry(entry, it, expire_at);
        }
    }

    MapIt insert_new(std::string&& key, std::string&& value) {
        auto [it, inserted] = sorted.try_emplace(std::move(key));
        Entry& entry = it->second;
        entry.value = std::move(value);
        entry.expiry_it = expiryIndex.end();
        hashIndex.emplace(std::string_view(it->first), it);
        return it;
    }

    void unlink_expiry(Entry& entry) noexcept {
        if (entry.expiry_it != expiryIndex.end()) {
            expiryIndex.erase(entry.expiry_it);
            entry.expiry_it = expiryIndex.end();
        }
    }

    void link_expiry(Entry& entry, MapIt it, time_point expire_at) {
        entry.expiry_it = expiryIndex.emplace(expire_at, std::string_view(it->first));
    }

    [[nodiscard]]
    std::unique_lock<std::shared_mutex> writer_lock() {
        writers_count.fetch_add(1, std::memory_order_release);
        std::unique_lock<std::shared_mutex> lock(mtx);
        const auto prev = writers_count.fetch_sub(1, std::memory_order_release);
        if (prev == 1) writers_count.notify_all();
        return lock;
    }
};