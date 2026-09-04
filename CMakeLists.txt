cmake_minimum_required(VERSION 3.20)

project(udp-ingestion-bench LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_executable(phase0_smoke src/main.cpp)
target_link_libraries(phase0_smoke PRIVATE Threads::Threads)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(phase0_smoke PRIVATE -Wall -Wextra -Wpedantic)
endif()
