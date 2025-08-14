cmake_minimum_required(VERSION 3.15)
project(KVStorage LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED true)

enable_testing()

add_library(kvstorage INTERFACE)
target_include_directories(kvstorage INTERFACE ${CMAKE_TARGAT_SOURCE_DIR}/src)

find_package(GTest CONFIG REQUIRED)

add_executable(tests tests/KVStorage_test.cpp)
target_link_libraries(tests PRIVATE kvstorage GTest::gtest GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(tests)