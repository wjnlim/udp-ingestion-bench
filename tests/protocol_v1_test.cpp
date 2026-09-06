#include "udp_ingestion/protocol_v1.hpp"
#include "udp_ingestion/synthetic_message.hpp"

#include <array>
#include <cstdint>
#include <iostream>
#include <limits>

namespace {

int failures = 0;

void expect(bool condition, const char* description) {
    if (!condition) {
        std::cerr << "FAIL: " << description << '\n';
        ++failures;
    }
}

bool equal(const udp_ingestion::MarketDataMessage& left,
           const udp_ingestion::MarketDataMessage& right) {
    return left.sequence == right.sequence && left.order_id == right.order_id &&
           left.price == right.price && left.instrument_id == right.instrument_id &&
           left.quantity == right.quantity &&
           left.message_type == right.message_type && left.side == right.side;
}

void test_round_trip(std::int64_t price,
                     udp_ingestion::MessageType type,
                     udp_ingestion::Side side) {
    const udp_ingestion::MarketDataMessage input{
        42, 0x0102030405060708ULL, price, 1234, 5678, type, side};
    const auto encoded = udp_ingestion::encode_protocol_v1(input);
    udp_ingestion::MarketDataMessage decoded{};
    expect(udp_ingestion::decode_protocol_v1(encoded.data(), encoded.size(), decoded),
           "valid message decodes");
    expect(equal(input, decoded), "encode/decode round-trip preserves every field");
}

void test_known_byte_order() {
    const udp_ingestion::MarketDataMessage input{
        0x0102030405060708ULL,
        0x1112131415161718ULL,
        0x2122232425262728LL,
        0x31323334U,
        0x41424344U,
        udp_ingestion::MessageType::Cancel,
        udp_ingestion::Side::Sell,
    };
    const std::array<std::uint8_t, 34> expected{
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
        0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
        0x31, 0x32, 0x33, 0x34,
        0x41, 0x42, 0x43, 0x44,
        0x02, 0x01,
    };
    expect(udp_ingestion::encode_protocol_v1(input) == expected,
           "known values use the specified big-endian layout");
}

void test_invalid_input() {
    udp_ingestion::ProtocolV1Buffer encoded{};
    udp_ingestion::MarketDataMessage output{};
    expect(!udp_ingestion::decode_protocol_v1(encoded.data(), encoded.size() - 1, output),
           "incorrect payload size is rejected");
    encoded[32] = 4;
    expect(!udp_ingestion::decode_protocol_v1(encoded.data(), encoded.size(), output),
           "invalid message type is rejected");
}

void test_generation() {
    const auto channel_0 = udp_ingestion::make_synthetic_message(0, 1, 4);
    const auto channel_1 = udp_ingestion::make_synthetic_message(1, 1, 4);
    expect(channel_0.sequence == 1 && channel_1.sequence == 1,
           "channels accept independent sequence numbers");
    expect(channel_0.instrument_id != channel_1.instrument_id,
           "generation distributes instruments across channels");
    expect(channel_0.message_type != channel_1.message_type,
           "generation produces multiple message types");
    expect(channel_0.side != channel_1.side, "generation produces both sides");
}

}  // namespace

int main() {
    static_assert(udp_ingestion::kProtocolV1WireSize == 34,
                  "Protocol v1 wire size must remain 34 bytes");
    test_round_trip(1'234'567, udp_ingestion::MessageType::Add,
                    udp_ingestion::Side::Buy);
    test_round_trip(1'234'567, udp_ingestion::MessageType::Modify,
                    udp_ingestion::Side::Sell);
    test_round_trip(1'234'567, udp_ingestion::MessageType::Cancel,
                    udp_ingestion::Side::Buy);
    test_round_trip(-1'234'567, udp_ingestion::MessageType::Trade,
                    udp_ingestion::Side::Sell);
    test_round_trip(std::numeric_limits<std::int64_t>::min(),
                    udp_ingestion::MessageType::Modify,
                    udp_ingestion::Side::Buy);
    test_known_byte_order();
    test_invalid_input();
    test_generation();

    if (failures != 0) {
        std::cerr << failures << " test assertion(s) failed\n";
        return 1;
    }
    std::cout << "all Protocol v1 tests passed\n";
    return 0;
}
