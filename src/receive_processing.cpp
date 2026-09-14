#include "udp_ingestion/receive_processing.hpp"

#include <limits>

namespace udp_ingestion {
namespace {

void track_message_sequence(const MarketDataMessage& message, 
                                        ChannelState& state) {
    ++state.valid_packets;
    const auto sequence = message.sequence;

    if (state.sequence_exhausted || sequence < state.expected_sequence) {
        ++state.late_or_duplicate_packets;
    } else {
        if (sequence > state.expected_sequence) {
            ++state.gap_events;
            state.missing_packets += sequence - state.expected_sequence;
        }

        if (sequence == std::numeric_limits<std::uint64_t>::max()) {
            state.sequence_exhausted = true;
        } else {
            state.expected_sequence = sequence + 1;
        }
    }
}

void accumulate_checksum(
    const MarketDataMessage& message, std::uint64_t& checksum) {
    // Simulates minimal processing; not a runtime correctness comparison.
    checksum += message.sequence;
    checksum += message.order_id;
    checksum += static_cast<std::uint64_t>(message.price);
    checksum += message.instrument_id;
    checksum += message.quantity;
    checksum += static_cast<std::uint64_t>(message.message_type);
    checksum += static_cast<std::uint64_t>(message.side);
}
} // namespace

bool decode_and_track_datagram(const std::uint8_t* data, std::size_t size,
            MarketDataMessage& message, ChannelState& state) {
    ++state.received_packets;

    if (!decode_protocol_v1(data, size, message)) {
        ++state.invalid_packets;
        return false;
    }

    track_message_sequence(message, state);
    return true;
}

void process_downstream_message(const MarketDataMessage& message,
                                            DownstreamState& state) {
    accumulate_checksum(message, state.checksum);
    ++state.processed_packets;
}

// Compatibility entry point for existing callers.
void process_message(const MarketDataMessage& message, ChannelState& state) {
    track_message_sequence(message, state);
    accumulate_checksum(message, state.checksum);
}


// Compatibility entry point for existing callers.
bool process_datagram(const std::uint8_t* data, std::size_t size, 
                                                ChannelState& state) {
    MarketDataMessage message{};
    if (!decode_and_track_datagram(data, size, message, state)) {
        return false;
    }

    accumulate_checksum(message, state.checksum);
    return true;
}

} //namespace udp_ingestion
