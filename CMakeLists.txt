cmake_minimum_required(VERSION 3.20)

project(udp-ingestion-bench LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

function(enable_project_warnings target)
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic)
    endif()
endfunction()

add_executable(phase0_smoke src/main.cpp)
target_link_libraries(phase0_smoke PRIVATE Threads::Threads)
enable_project_warnings(phase0_smoke)

add_library(protocol_v1
    src/protocol_v1.cpp
    src/synthetic_message.cpp
)
target_include_directories(protocol_v1 PUBLIC include)
enable_project_warnings(protocol_v1)

add_executable(synthetic_publisher src/publisher.cpp)
target_link_libraries(synthetic_publisher PRIVATE protocol_v1)
enable_project_warnings(synthetic_publisher)

add_library(receive_processing
    src/receive_processing.cpp
)
target_link_libraries(receive_processing PUBLIC protocol_v1)
enable_project_warnings(receive_processing)

add_executable(dedicated_receiver
    src/dedicated_receiver.cpp
    src/receive_worker.cpp
    src/cpu_affinity.cpp
)
target_link_libraries(dedicated_receiver
    PRIVATE receive_processing Threads::Threads
)
enable_project_warnings(dedicated_receiver)

include(CTest)
if(BUILD_TESTING)
    add_executable(protocol_v1_test tests/protocol_v1_test.cpp)
    target_link_libraries(protocol_v1_test PRIVATE protocol_v1)
    enable_project_warnings(protocol_v1_test)
    add_test(NAME protocol_v1_test COMMAND protocol_v1_test)

    add_executable(udp_validation_receiver tests/udp_validation_receiver.cpp)
    target_link_libraries(udp_validation_receiver PRIVATE protocol_v1)
    enable_project_warnings(udp_validation_receiver)

    add_executable(receive_processing_test tests/receive_processing_test.cpp)
    target_link_libraries(receive_processing_test PRIVATE receive_processing)
    enable_project_warnings(receive_processing_test)
    add_test(NAME receive_processing_test COMMAND receive_processing_test)
endif()
