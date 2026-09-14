#include "udp_ingestion/stop_event.hpp"

#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <stdexcept>
#include <system_error>

namespace udp_ingestion {

StopEvent::StopEvent() {
    fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (fd_ < 0) {
        const int error = errno;
        throw std::system_error(error, std::generic_category(), "eventfd");
    }
}

StopEvent::~StopEvent() {
    if (fd_ >= 0) {
        ::close(fd_);
    }
}

void StopEvent::notify() {
    const std::uint64_t value = 1;

    for (;;) {
        const auto size = ::write(fd_, &value, sizeof(value));

        if (size == static_cast<ssize_t>(sizeof(value))) {
            return;
        }

        if (size < 0) {
            const int error = errno;

            if (error == EINTR) {
                continue;
            }
            if (error == EAGAIN) {
                return;
            }

            throw std::system_error(error, std::generic_category(), 
                                    "write stop event");
        }
        throw std::runtime_error("short write to stop event");
    }
}

} // namespace udp_ingestion