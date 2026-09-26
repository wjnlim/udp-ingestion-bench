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

add_executable(synthetic_publisher 
    src/publisher.cpp
    src/cpu_affinity.cpp
)
target_link_libraries(synthetic_publisher 
    PRIVATE protocol_v1 Threads::Threads)
enable_project_warnings(synthetic_publisher)

add_library(receive_processing
    src/receive_processing.cpp
)
target_link_libraries(receive_processing PUBLIC protocol_v1)
enable_project_warnings(receive_processing)

add_executable(dedicated_receiver
    src/dedicated_receiver.cpp
    src/receiver_reporting.cpp
    src/receive_worker.cpp
    src/downstream_worker.cpp
    src/run_timing.cpp
    src/latency.cpp
    src/timestamped_receive.cpp
    src/cpu_affinity.cpp
)
target_link_libraries(dedicated_receiver
    PRIVATE receive_processing Threads::Threads
)
enable_project_warnings(dedicated_receiver)

add_executable(epoll_receiver
    src/epoll_receiver.cpp
    src/receiver_reporting.cpp
    src/epoll_receive_worker.cpp
    src/downstream_worker.cpp
    src/run_timing.cpp
    src/latency.cpp
    src/stop_event.cpp
    src/timestamped_receive.cpp
    src/cpu_affinity.cpp
)
target_link_libraries(epoll_receiver
    PRIVATE receive_processing Threads::Threads
)
enable_project_warnings(epoll_receiver)

option(BUILD_LIBEVENT_RECEIVER
    "Build the libevent validation receiver"
    OFF
)

if (BUILD_LIBEVENT_RECEIVER)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(LIBEVENT REQUIRED IMPORTED_TARGET libevent>=2.1)

    add_executable(libevent_receiver
        src/libevent_receiver.cpp
        src/receiver_reporting.cpp
        src/libevent_receive_worker.cpp
        src/downstream_worker.cpp
        src/run_timing.cpp
        src/latency.cpp
        src/stop_event.cpp
        src/timestamped_receive.cpp
        src/cpu_affinity.cpp
    )
    target_link_libraries(libevent_receiver
        PRIVATE receive_processing Threads::Threads PkgConfig::LIBEVENT
    )
    enable_project_warnings(libevent_receiver)
endif()

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

    add_executable(epoll_receive_worker_test
        tests/epoll_receive_worker_test.cpp
        src/epoll_receive_worker.cpp
        src/stop_event.cpp
        src/timestamped_receive.cpp
        src/cpu_affinity.cpp
    )
    target_link_libraries(epoll_receive_worker_test
        PRIVATE receive_processing Threads::Threads
    )
    enable_project_warnings(epoll_receive_worker_test)
    add_test(
        NAME epoll_receive_worker_test
        COMMAND epoll_receive_worker_test
    )
    set_tests_properties(epoll_receive_worker_test PROPERTIES
        TIMEOUT 30
        RUN_SERIAL TRUE
    )

    add_executable(spsc_queue_test
        tests/spsc_queue_test.cpp
    )
    target_include_directories(spsc_queue_test PRIVATE include)
    target_link_libraries(spsc_queue_test PRIVATE Threads::Threads)
    enable_project_warnings(spsc_queue_test)
    add_test(NAME spsc_queue_test COMMAND spsc_queue_test)
    set_tests_properties(spsc_queue_test PROPERTIES
        TIMEOUT 30
    )

    add_executable(downstream_worker_test
        tests/downstream_worker_test.cpp
        src/downstream_worker.cpp
        src/run_timing.cpp
        src/latency.cpp
        src/cpu_affinity.cpp
    )
    target_link_libraries(downstream_worker_test
        PRIVATE receive_processing Threads::Threads
    )
    enable_project_warnings(downstream_worker_test)
    add_test(
        NAME downstream_worker_test
        COMMAND downstream_worker_test
    )
    set_tests_properties(downstream_worker_test PROPERTIES
        TIMEOUT 30
    )

    add_executable(timestamped_receive_test
        tests/timestamped_receive_test.cpp
        src/timestamped_receive.cpp
    )
    target_include_directories(timestamped_receive_test PRIVATE include)
    enable_project_warnings(timestamped_receive_test)
    add_test(
        NAME timestamped_receive_test
        COMMAND timestamped_receive_test
    )
    set_tests_properties(timestamped_receive_test PROPERTIES
        TIMEOUT 30
    )

    add_executable(timestamped_receive_socket_test
        tests/timestamped_receive_socket_test.cpp
        src/timestamped_receive.cpp
    )
    target_include_directories(timestamped_receive_socket_test PRIVATE include)
    enable_project_warnings(timestamped_receive_socket_test)
    add_test(
        NAME timestamped_receive_socket_test
        COMMAND timestamped_receive_socket_test
    )
    set_tests_properties(timestamped_receive_socket_test PROPERTIES
        TIMEOUT 30
    )

    add_executable(latency_clock_test
        tests/latency_clock_test.cpp
        src/latency.cpp
    )
    target_include_directories(latency_clock_test PRIVATE include)
    enable_project_warnings(latency_clock_test)
    add_test(
        NAME latency_clock_test
        COMMAND latency_clock_test
    )
    set_tests_properties(latency_clock_test PROPERTIES
        TIMEOUT 30
    )

    add_executable(latency_samples_test
        tests/latency_samples_test.cpp
        src/latency.cpp
    )
    target_include_directories(latency_samples_test PRIVATE include)
    enable_project_warnings(latency_samples_test)
    add_test(
        NAME latency_samples_test
        COMMAND latency_samples_test
    )
    set_tests_properties(latency_samples_test PROPERTIES
        TIMEOUT 30
    )

    add_executable(latency_statistics_test
        tests/latency_statistics_test.cpp
        src/latency.cpp
    )
    target_include_directories(latency_statistics_test PRIVATE include)
    enable_project_warnings(latency_statistics_test)
    add_test(
        NAME latency_statistics_test
        COMMAND latency_statistics_test
    )
    set_tests_properties(latency_statistics_test PROPERTIES
        TIMEOUT 30
    )

    add_executable(run_timing_test
        tests/run_timing_test.cpp
        src/run_timing.cpp
    )
    target_include_directories(run_timing_test PRIVATE include)
    enable_project_warnings(run_timing_test)
    add_test(
        NAME run_timing_test
        COMMAND run_timing_test
    )
    set_tests_properties(run_timing_test PROPERTIES
        TIMEOUT 30
    )

    if(BUILD_LIBEVENT_RECEIVER)
        add_executable(libevent_backend_probe
            tests/libevent_backend_probe.cpp
        )
        target_link_libraries(libevent_backend_probe
            PRIVATE PkgConfig::LIBEVENT
        )
        enable_project_warnings(libevent_backend_probe)

        add_test(
            NAME libevent_backend_probe
            COMMAND libevent_backend_probe
        )
        set_tests_properties(libevent_backend_probe PROPERTIES
            TIMEOUT 10
        )

                add_executable(libevent_receive_worker_test
            tests/libevent_receive_worker_test.cpp
            src/libevent_receive_worker.cpp
            src/stop_event.cpp
            src/timestamped_receive.cpp
            src/cpu_affinity.cpp
        )
        target_link_libraries(libevent_receive_worker_test
            PRIVATE receive_processing Threads::Threads PkgConfig::LIBEVENT
        )
        enable_project_warnings(libevent_receive_worker_test)

        add_test(
            NAME libevent_receive_worker_test
            COMMAND libevent_receive_worker_test
        )
        set_tests_properties(libevent_receive_worker_test PROPERTIES
            TIMEOUT 10
            RUN_SERIAL TRUE
        )
    endif()

endif()
