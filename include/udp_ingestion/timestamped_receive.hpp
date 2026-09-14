#pragma once

#include "udp_ingestion/protocol_v1.hpp"

#include <sys/socket.h>
#include <sys/types.h>

#include <array>
#include <cstdint>

namespace udp_ingestion {

struct ReceivedDatagram {
    // One extra byte for detecting oversized datagrams
    std::array<std::uint8_t, kProtocolV1BufSize + 1> payload{};
    // Kernel software RX time: nanoseconds since the CLOCK_REALTIME epoch
    std::int64_t rx_timestamp_ns = 0;
};

// Enables SO_TIMESTAMPNS_NEW.
void enable_rx_timestamp(int fd);

// Extracts the SO_TIMESTAMPNS_NEW ancillary timestamp.
std::int64_t extract_rx_timestamp_ns(msghdr& message);

// Performs one nonblocking recvmsg()
ssize_t receive_timestamped_datagram(int fd, ReceivedDatagram& output);

} // namespace udp_ingestion