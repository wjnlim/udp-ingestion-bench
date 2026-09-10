#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "udp_ingestion/cpu_affinity.hpp"

#include <pthread.h>
#include <sched.h>

#include <stdexcept>
#include <system_error>

namespace udp_ingestion {
    
void pin_current_thread(int cpu, const std::string& role) {
    if (cpu < 0 || cpu >= CPU_SETSIZE) {
        throw std::invalid_argument(role + ": CPU " + std::to_string(cpu)
                                    + " is outside the supported CPU set");
    }

    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(cpu, &cpu_set);

    const int error = ::pthread_setaffinity_np(
                        ::pthread_self(), sizeof(cpu_set), &cpu_set);
    
    if (error != 0) {
        throw std::system_error(error, std::generic_category(),
                        role + ": failed to pin to CPU " + std::to_string(cpu));
    }
}

} // namespace udp_ingestion
