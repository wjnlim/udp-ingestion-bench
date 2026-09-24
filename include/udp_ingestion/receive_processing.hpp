#pragma once

#include "udp_ingestion/protocol_v1.hpp"

#include <cstddef>
#include <cstdint>

namespace udp_ingestion {

struct ChannelState {
    std::uint64_t expected_sequence = 1;
    bool sequence_exhausted = false;

    std::uint64_t received_packets = 0;
    // successfully decoded packet, including late/duplicate packets
    std::uint64_t valid_packets = 0;
    std::uint64_t invalid_packets = 0;
    // number of (Received seq > Expected seq) events
    std::uint64_t gap_events = 0;
    // cumulative value of the observed forward gap
    // Even if missing packets arrive later, this is not decremented
    std::uint64_t missing_packets = 0;
    // number of (Received seq < Expected seq) packets
    std::uint64_t late_or_duplicate_packets = 0;

    // Temporary compatibility field for the existing non-staged callers.
    // The staged RX path does not update this field.
    std::uint64_t checksum = 0;
};

struct DownstreamState {
    std::uint64_t processed_packets = 0;
    std::uint64_t checksum = 0;
};

// Counts and decodes a datagram, then updates RX sequence statistics.
bool decode_and_track_datagram(const std::uint8_t* data, std::size_t size,
                               MarketDataMessage& message, ChannelState& state);

// Updates only downstream-owned processing state.
void process_downstream_message(const MarketDataMessage& message, DownstreamState& state);

// Processes an already decoded message
void process_message(const MarketDataMessage& message, ChannelState& state);

// Decodes and processes a datagram
bool process_datagram(const std::uint8_t* data, std::size_t size, ChannelState& state);

} // namespace udp_ingestion
