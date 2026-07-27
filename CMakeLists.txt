cmake_minimum_required(VERSION 3.20)
project(tsdb)
set(CMAKE_OSX_SYSROOT /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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
add_executable(tsdb main.cpp storage.cpp network.cpp)
target_compile_options(tsdb PRIVATE -Wall -Wextra)
if(URING_LIB)
    target_link_libraries(tsdb ${URING_LIB})
endif()

# Interactive CLI Tool Executable
add_executable(tsdb-cli cli.cpp)
target_compile_options(tsdb-cli PRIVATE -Wall -Wextra)