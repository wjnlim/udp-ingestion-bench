#include "udp_ingestion/receive_processing.hpp"

#include <limits>

namespace udp_ingestion {

void process_message(const MarketDataMessage& message, ChannelState& state) {
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
    // checksum field is for only simulating minimal message processing 
    // by accumulating decoded field values. Not for an actual correctness check.
    state.checksum += message.sequence;
    state.checksum += message.order_id;
    state.checksum += static_cast<std::uint64_t>(message.price);
    state.checksum += message.instrument_id;
    state.checksum += message.quantity;
    state.checksum += static_cast<std::uint64_t>(message.message_type);
    state.checksum += static_cast<std::uint64_t>(message.side);
}


bool process_datagram(const std::uint8_t* data, std::size_t size, 
                                                ChannelState& state) {
    ++state.received_packets;

    MarketDataMessage message{};
    if (!decode_protocol_v1(data, size, message)) {
        ++state.invalid_packets;
        return false;
    }

    process_message(message, state);
    return true;
}

} //namespace udp_ingestion
