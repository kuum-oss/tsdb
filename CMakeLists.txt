cmake_minimum_required(VERSION 3.20)
project(tsdb)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    add_compile_options(-fmodules-ts)
endif()

option(WITH_URING "Build with io_uring support" ON)

if(WITH_URING)
    find_library(URING_LIB uring)
    if(URING_LIB)
        add_definitions(-DTSDB_HAS_URING=1)
    else()
        message(STATUS "liburing not found, falling back to epoll")
    endif()
endif()

# Database Server Executable
add_executable(tsdb protocol.cppm main.cpp storage.cpp network.cpp
        query.hpp)
set_target_properties(tsdb PROPERTIES UNITY_BUILD ON)
if(URING_LIB)
    target_link_libraries(tsdb ${URING_LIB})
endif()

# Interactive CLI Tool Executable
add_executable(tsdb-cli protocol.cppm cli.cpp)
set_target_properties(tsdb-cli PROPERTIES UNITY_BUILD ON)
include_directories(SYSTEM "/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include/c++/v1")