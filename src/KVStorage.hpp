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
    
    explicit KVStorage(std::span<std::tuple<std::string /*key*/, std::string /*value*/,
    uint32_t /*ttl*/>> entries, Clock clock = Clock()) {
        // TODO: implement me
    }
    
    ~KVStorage() {
        // TODO: implement me
    }

    void set(std::string key, std::string value, uint32_t ttl) {
        auto lock = writer_lock();

        const time_point expire_at = compute_expire_time(ttl);

        if (auto hit = hashIndex.find(std::string_view(key)); hit != hashIndex.end()) {
            update_value(hit->second, std::move(value), expire_at);
        } else {
            MapIt it = insert_new(std::move(key), std::move(value));
            if (expire_at != time_point::max()) {
                link_expiry(it->second, it, expire_at);
            }
        }
    }

    bool remove(std::string_view key) {
        // TODO: implement me
        return false;
    }

    std::optional<std::string> get(std::string_view key) const {
        // TODO: implement me
        return std::nullopt;
    }

    std::vector<std::pair<std::string, std::string>> getManySorted(std::string_view key,
    uint32_t count) const {
        // TODO: implement me
        return {};
    }

    std::optional<std::pair<std::string, std::string>> removeOneExpiredEntry() {
        // TODO: implement me
        return std::nullopt;
    }
private:
    // TODO: implement me
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


    bool is_eternal_nolock(const Entry& e) const noexcept {
        return e.expiry_it == expiryIndex.end();
    }
    bool is_expired_now_nolock(const Entry& e) const {
        if (is_eternal_nolock(e)) return false;
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
        writers_count.fetch_add(1, std::memory_order_acq_rel);
        std::unique_lock<std::shared_mutex> lock(mtx);
        const auto prev = writers_count.fetch_sub(1, std::memory_order_acq_rel);
        if (prev == 1) writers_count.notify_all();
        return lock;
    }
};