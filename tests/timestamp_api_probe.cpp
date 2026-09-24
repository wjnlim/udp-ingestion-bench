#include <arpa/inet.h>
#include <linux/time_types.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <system_error>

namespace {

void require_success(int result, const char* operation) {
    if (result < 0) {
        const int error = errno;
        throw std::system_error(error, std::generic_category(), operation);
    }
}

timespec realtime_now() {
    timespec value{};
    require_success(::clock_gettime(CLOCK_REALTIME, &value), "clock_gettime");
    return value;
}

void run_probe(int fd) {
    const int enabled = 1;
    require_success(::setsockopt(fd, SOL_SOCKET, SO_TIMESTAMPNS_NEW, &enabled, sizeof(enabled)),
                    "setsockopt SO_TIMESTAMPNS_NEW");

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;

    require_success(::bind(fd, reinterpret_cast<const sockaddr*>(&address), sizeof(address)),
                    "bind");

    socklen_t address_size = sizeof(address);
    require_success(::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &address_size),
                    "getsockname");

    std::cout << "SO_TIMESTAMPNS_NEW=" << SO_TIMESTAMPNS_NEW
              << "\nSCM_TIMESTAMPNS=" << SCM_TIMESTAMPNS
              << "\nsizeof(__kernel_timespec)=" << sizeof(__kernel_timespec)
              << "\nsizeof(timespec)=" << sizeof(timespec) << '\n';

    const char sent_byte = 'T';
    const auto before = realtime_now();

    const auto sent = ::sendto(fd, &sent_byte, sizeof(sent_byte), 0,
                               reinterpret_cast<const sockaddr*>(&address), sizeof(address));

    if (sent < 0) {
        const int error = errno;
        throw std::system_error(error, std::generic_category(), "sendto");
    }
    if (sent != static_cast<ssize_t>(sizeof(sent_byte))) {
        throw std::runtime_error("unexpected send size");
    }

    pollfd readiness{};
    readiness.fd = fd;
    readiness.events = POLLIN;

    const int ready = ::poll(&readiness, 1, 2000);
    require_success(ready, "poll");

    if (ready == 0) {
        throw std::runtime_error("receive readiness timeout");
    }
    if ((readiness.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
        throw std::runtime_error("unexpected poll error");
    }
    if ((readiness.revents & POLLIN) == 0) {
        throw std::runtime_error("socket is not readable");
    }

    char received_byte{};
    iovec payload{};
    payload.iov_base = &received_byte;
    payload.iov_len = sizeof(received_byte);

    alignas(cmsghdr) std::array<unsigned char, 512> control{};

    msghdr message{};
    message.msg_iov = &payload;
    message.msg_iovlen = 1;
    message.msg_control = control.data();
    message.msg_controllen = control.size();

    const auto received = ::recvmsg(fd, &message, MSG_DONTWAIT);
    if (received < 0) {
        const int error = errno;
        throw std::system_error(error, std::generic_category(), "recvmsg");
    }

    const auto after = realtime_now();

    if ((message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) != 0) {
        throw std::runtime_error("payload or control data truncated");
    }
    if (received != static_cast<ssize_t>(sizeof(received_byte)) || received_byte != sent_byte) {
        throw std::runtime_error("unexpected received payload");
    }

    __kernel_timespec rx{};
    bool found = false;

    for (auto* cmsg = CMSG_FIRSTHDR(&message); cmsg != nullptr;
         cmsg = CMSG_NXTHDR(&message, cmsg)) {
        if (cmsg->cmsg_len < CMSG_LEN(0)) {
            throw std::runtime_error("invalid control message length");
        }

        std::cout << "cmsg_level=" << cmsg->cmsg_level << " cmsg_type=" << cmsg->cmsg_type
                  << " cmsg_len=" << cmsg->cmsg_len
                  << " payload_bytes=" << cmsg->cmsg_len - CMSG_LEN(0) << '\n';

        if (cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SO_TIMESTAMPNS_NEW) {
            continue;
        }

        if (found) {
            throw std::runtime_error("duplicate NEW timestamp");
        }
        if (cmsg->cmsg_len != CMSG_LEN(sizeof(rx))) {
            throw std::runtime_error("unexpected NEW timestamp size");
        }

        std::memcpy(&rx, CMSG_DATA(cmsg), sizeof(rx));
        found = true;
    }

    if (!found) {
        throw std::runtime_error("NEW timestamp control message not found");
    }
    if (rx.tv_nsec < 0 || rx.tv_nsec >= 1'000'000'000) {
        throw std::runtime_error("invalid timestamp nanoseconds");
    }

    std::cout << "before_sec=" << before.tv_sec << " before_nsec=" << before.tv_nsec
              << "\nrx_sec=" << rx.tv_sec << " rx_nsec=" << rx.tv_nsec
              << "\nafter_sec=" << after.tv_sec << " after_nsec=" << after.tv_nsec << '\n';

    const bool after_send_start =
        rx.tv_sec > before.tv_sec || (rx.tv_sec == before.tv_sec && rx.tv_nsec >= before.tv_nsec);

    const bool before_receive_end =
        rx.tv_sec < after.tv_sec || (rx.tv_sec == after.tv_sec && rx.tv_nsec <= after.tv_nsec);

    if (!after_send_start || !before_receive_end) {
        throw std::runtime_error("RX timestamp outside CLOCK_REALTIME interval");
    }

    std::cout << "timestamp API probe passed\n";
}

} // namespace

int main() {
    const int fd = ::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);

    if (fd < 0) {
        const int error = errno;
        std::cerr << "socket: " << std::system_error(error, std::generic_category()).what() << '\n';
        return 1;
    }

    try {
        run_probe(fd);
    } catch (const std::exception& error) {
        ::close(fd);
        std::cerr << "probe error: " << error.what() << '\n';
        return 1;
    }

    ::close(fd);
    return 0;
}