#pragma once
#include <tuple>
#include <cstdint>
#include <string>
#include <optional>
#include <vector>


template<typename Clock>
class KVStorage {

public:
    explicit KVStorage(std::span<std::tuple<std::string key, std::string value,
    uint32_t ttl>> entries, Clock clock = Clock()) {
        // TODO: implement me
    }
    
    ~KVStorage() {
        // TODO: implement me
    }

    void set(std::string key, std::string value, uint32_t ttl) {
        // TODO: implement me
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
};