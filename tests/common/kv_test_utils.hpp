#pragma once
#include "kv_test_clock.hpp"
#include <thread>

struct StartStopGate {
    std::atomic<bool> go{false}, stop{false};
    void start() { go.store(true, std::memory_order_release); go.notify_all(); }
    void request_stop() { stop.store(true, std::memory_order_release); }
    void wait_go() {
        bool v = go.load(std::memory_order_acquire);
        while (!v) { go.wait(v, std::memory_order_acquire); v = go.load(std::memory_order_acquire); }
    }
};

template<class F>
std::vector<std::thread> spawn_readers(int n, StartStopGate& gate, F&& body) {
    std::vector<std::thread> v; v.reserve(n);
    for (int i=0;i<n;++i) {
        v.emplace_back([&]{
            gate.wait_go();
            while (!gate.stop.load(std::memory_order_acquire)) body();
        });
    }
    return v;
}