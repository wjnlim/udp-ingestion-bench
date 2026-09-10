#include "udp_ingestion/receive_processing.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>

namespace {

using udp_ingestion::ChannelState;
using udp_ingestion::MarketDataMessage;
using udp_ingestion::MessageType;
using udp_ingestion::Side;

int failures = 0;

void expect(bool condition, const char* description) {
    if (!condition) {
        std::cerr << "FAIL: " << description << '\n';
        ++failures;
    }
}

MarketDataMessage make_message(std::uint64_t sequence) {
    return MarketDataMessage {
        sequence,
        10,
        10'000,
        2,
        3,
        MessageType::Modify,
        Side::Sell,
    };
}

void deliver(std::uint64_t sequence, ChannelState& state) {
    const auto pbuf = udp_ingestion::encode_protocol_v1(make_message(sequence));
    expect(udp_ingestion::process_datagram(pbuf.data(), pbuf.size(), state),
        "valid datagram is accepted");
}

void test_normal_sequence() {
    ChannelState state{};

    expect(state.expected_sequence == 1, "initial expected sequence is one");
    expect(!state.sequence_exhausted, "initial sequence is not exhausted");

    deliver(1, state);
    deliver(2, state);

    expect(state.received_packets == 2, "normal received count");
    expect(state.valid_packets == 2, "normal valid count");
    expect(state.invalid_packets == 0, "normal invalid count");
    expect(state.expected_sequence == 3, "normal sequence advances");
    expect(state.gap_events == 0, "normal sequence has no gaps");
    expect(state.missing_packets == 0, "normal sequence has no missing packets");
    expect(state.late_or_duplicate_packets == 0,
           "normal sequence has no late or duplicate packets");

    // Field sums for these messages are 10018 and 10019.
    expect(state.checksum == 20'037, "decoded fields update checksum");
}

void test_gaps_and_late_packets() {
    ChannelState state{};

    deliver(1, state);
    deliver(4, state);

    expect(state.expected_sequence == 5, "forward gap advances expectation");
    expect(state.gap_events == 1, "first gap event");
    expect(state.missing_packets == 2, "sequences two and three are missing");

    deliver(8, state);

    expect(state.expected_sequence == 9, "second gap advances expectation");
    expect(state.gap_events == 2, "second gap event");
    expect(state.missing_packets == 5, "missing count accumulates both gaps");

    deliver(3, state);
    deliver(8, state);

    expect(state.expected_sequence == 9,
           "late and duplicate packets do not move expectation");
    expect(state.late_or_duplicate_packets == 2,
           "late and duplicate packets share one counter");
    expect(state.missing_packets == 5,
           "late arrival does not reduce observed missing count");
    expect(state.gap_events == 2,
           "late and duplicate packets do not create gaps");

    deliver(9, state);

    expect(state.expected_sequence == 10, "normal processing resumes");
    expect(state.received_packets == 6, "all six datagrams are counted");
    expect(state.valid_packets == 6, "late and duplicate packets are valid");
    expect(state.invalid_packets == 0, "sequence anomalies are not invalid");

    // Six messages: sequence sum 33 plus six times the other-field sum 10017.
    expect(state.checksum == 60'135,
           "checksum includes late and duplicate messages");
}

void expect_invalid(const std::uint8_t* data, std::size_t size, 
                                            ChannelState& state) {
    const auto before = state;

    expect(!udp_ingestion::process_datagram(data, size, state),
           "invalid datagram is rejected");
    expect(state.received_packets == before.received_packets + 1,
           "invalid datagram increments received count");
    expect(state.invalid_packets == before.invalid_packets + 1,
           "invalid datagram increments invalid count");
    expect(state.valid_packets == before.valid_packets,
           "invalid datagram does not increment valid count");
    expect(state.expected_sequence == before.expected_sequence &&
               state.sequence_exhausted == before.sequence_exhausted &&
               state.gap_events == before.gap_events &&
               state.missing_packets == before.missing_packets &&
               state.late_or_duplicate_packets ==
                   before.late_or_duplicate_packets &&
               state.checksum == before.checksum,
           "invalid datagram leaves processing state unchanged");
}

void test_invalid_datagrams() {
    ChannelState state{};
    deliver(1, state);

    const auto pbuf =
        udp_ingestion::encode_protocol_v1(make_message(2));

    expect_invalid(nullptr, pbuf.size(), state);
    expect_invalid(pbuf.data(), 0, state);
    expect_invalid(pbuf.data(), pbuf.size() - 1, state);

    std::array<std::uint8_t, udp_ingestion::kProtocolV1BufSize + 1>
        oversized{};
    for (std::size_t i = 0; i < pbuf.size(); ++i) {
        oversized[i] = pbuf[i];
    }
    expect_invalid(oversized.data(), oversized.size(), state);

    auto bad_type = pbuf;
    bad_type[32] = 4;
    expect_invalid(bad_type.data(), bad_type.size(), state);

    auto bad_side = pbuf;
    bad_side[33] = 2;
    expect_invalid(bad_side.data(), bad_side.size(), state);

    deliver(2, state);

    expect(state.received_packets == 8, "total count includes invalid input");
    expect(state.valid_packets == 2, "two valid datagrams were processed");
    expect(state.invalid_packets == 6, "six invalid datagrams were rejected");
    expect(state.received_packets ==
               state.valid_packets + state.invalid_packets,
           "received equals valid plus invalid");
    expect(state.expected_sequence == 3,
           "valid processing continues after invalid input");
}

void test_independent_channels() {
    ChannelState state_ch0{};
    ChannelState state_ch1{};

    deliver(3, state_ch0);
    deliver(1, state_ch1);
    deliver(2, state_ch1);

    expect(state_ch0.received_packets == 1 &&
               state_ch0.expected_sequence == 4 &&
               state_ch0.gap_events == 1 &&
               state_ch0.missing_packets == 2,
           "channel zero tracks an initial gap");

    expect(state_ch1.received_packets == 2 &&
               state_ch1.expected_sequence == 3 &&
               state_ch1.gap_events == 0 &&
               state_ch1.missing_packets == 0 &&
               state_ch1.late_or_duplicate_packets == 0,
           "channel one is unaffected by channel zero");

    expect(state_ch0.checksum == 10'020 &&
               state_ch1.checksum == 20'037,
           "channels accumulate checksums independently");
}

void test_sequence_exhaustion() {
    ChannelState state{};
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    state.expected_sequence = maximum - 1;

    deliver(maximum - 1, state);
    expect(state.expected_sequence == maximum,
           "expectation can reach UINT64_MAX");
    expect(!state.sequence_exhausted, "UINT64_MAX is still expected");

    deliver(maximum, state);
    expect(state.sequence_exhausted, "UINT64_MAX exhausts sequence space");
    expect(state.expected_sequence == maximum,
           "expectation does not wrap to zero");

    deliver(maximum, state);
    deliver(0, state);

    expect(state.late_or_duplicate_packets == 2,
           "packets after exhaustion are late or duplicate");
    expect(state.gap_events == 0 && state.missing_packets == 0,
           "exhaustion does not create false gaps");
    expect(state.sequence_exhausted, "exhaustion remains set");
}

void test_signed_price_and_checksum_wrap() {
    auto message = make_message(1);
    message.price = -2;

    const auto wire = udp_ingestion::encode_protocol_v1(message);
    ChannelState negative_state{};

    expect(
        udp_ingestion::process_datagram(
            wire.data(), wire.size(), negative_state),
        "negative price decodes and is processed");

    // Other fields sum to 18; adding price -2 modulo 2^64 gives 16.
    expect(negative_state.checksum == 16,
           "negative price contributes to unsigned checksum");

    ChannelState wrapping_state{};
    wrapping_state.checksum = std::numeric_limits<std::uint64_t>::max();

    deliver(1, wrapping_state);

    expect(wrapping_state.checksum == 10'017,
           "checksum wraps modulo 2^64");
}

} // namespace

int main() {
    test_normal_sequence();
    test_gaps_and_late_packets();
    test_invalid_datagrams();
    test_independent_channels();
    test_sequence_exhaustion();
    test_signed_price_and_checksum_wrap();

    if (failures != 0) {
        std::cerr << failures << " test assertion(s) failed\n";
        return 1;
    }

    std::cout << "all receive processing tests passed\n";
    return 0;
}