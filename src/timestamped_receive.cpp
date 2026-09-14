#include "udp_ingestion/timestamped_receive.hpp"

#include <linux/time_types.h>
#include <sys/uio.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <system_error>

namespace udp_ingestion {

namespace {

constexpr std::int64_t kNanosecondsPerSecond = 1'000'000'000;

std::int64_t timestamp_to_ns(const __kernel_timespec& timestamp) {
    if (timestamp.tv_sec < 0 || timestamp.tv_nsec < 0 || 
        timestamp.tv_nsec >= kNanosecondsPerSecond) {
        throw std::runtime_error("invalid RX timestamp value");
    }

    constexpr auto maximum = std::numeric_limits<std::int64_t>::max();
    constexpr auto max_seconds = maximum / kNanosecondsPerSecond;
    constexpr auto max_nanoseconds = maximum % kNanosecondsPerSecond;

    if (timestamp.tv_sec > max_seconds
        || (timestamp.tv_sec == max_seconds
            && timestamp.tv_nsec > max_nanoseconds)) {
        throw std::runtime_error("RX timestamp exceeds int64 nanoseconds");
    }

    return static_cast<std::int64_t>(timestamp.tv_sec) * kNanosecondsPerSecond
           + static_cast<std::int64_t>(timestamp.tv_nsec);
}

} // namespace

void enable_rx_timestamp(int fd) {
    const int enabled = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_TIMESTAMPNS_NEW, &enabled, 
                                                sizeof(enabled)) != 0) {
        const int error = errno;
        throw std::system_error(error, std::generic_category(),
                                "setsockopt SO_TIMESTAMPNS_NEW");
    }
}

std::int64_t extract_rx_timestamp_ns(msghdr& message) {
    if ((message.msg_flags & MSG_CTRUNC) != 0) {
        throw std::runtime_error("RX ancillary data truncated");
    }

    if (message.msg_control == nullptr || 
        message.msg_controllen < CMSG_LEN(0)) {
        throw std::runtime_error("RX timestamp control message missing");
    }

    const auto* control = 
        static_cast<const unsigned char*>(message.msg_control);
    bool found = false;
    std::int64_t timestamp_ns = 0;

    for (auto* cmsg = CMSG_FIRSTHDR(&message); cmsg != nullptr;
                            cmsg = CMSG_NXTHDR(&message, cmsg)) {
        const auto offset = static_cast<std::size_t> (
            reinterpret_cast<const unsigned char*>(cmsg) - control);
        const auto remaining = message.msg_controllen - offset;

        if (cmsg->cmsg_len < CMSG_LEN(0) || cmsg->cmsg_len > remaining) {
            throw std::runtime_error("invalid RX control message length");
        }

        if (cmsg->cmsg_level != SOL_SOCKET ||
            cmsg->cmsg_type != SO_TIMESTAMPNS_NEW) {
            continue;
        }

        if (found) {
            throw std::runtime_error("duplicate RX timestamp");
        }

        __kernel_timespec timestamp{};

        if (cmsg->cmsg_len != CMSG_LEN(sizeof(timestamp))) {
            throw std::runtime_error("unexpected RX timestamp payload size");
        }

        std::memcpy(&timestamp, CMSG_DATA(cmsg), sizeof(timestamp));
        timestamp_ns = timestamp_to_ns(timestamp);
        found = true;
    }

    if (!found) {
        throw std::runtime_error("SO_TIMESTAMPNS_NEW control message missing");
    }

    return timestamp_ns;
}

ssize_t receive_timestamped_datagram(int fd, ReceivedDatagram& output) {
    iovec payload{};
    payload.iov_base = output.payload.data();
    payload.iov_len = output.payload.size();

    alignas(cmsghdr) 
        std::array<unsigned char, CMSG_SPACE(sizeof(__kernel_timespec))> control{};
    
    msghdr message{};
    message.msg_iov = &payload;
    message.msg_iovlen = 1;
    message.msg_control = control.data();
    message.msg_controllen = control.size();

    const auto received = ::recvmsg(fd, &message, MSG_DONTWAIT);

    if (received < 0) {
        return received;
    }

    output.rx_timestamp_ns = extract_rx_timestamp_ns(message);
    return received;
}

} // namespace udp_ingestion