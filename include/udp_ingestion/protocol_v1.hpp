#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace udp_ingestion {

enum class MessageType : std::uint8_t {
    Add = 0,
    Modify = 1,
    Cancel = 2,
    Trade = 3,
};

enum class Side : std::uint8_t {
    Buy = 0,
    Sell = 1,
};

struct MarketDataMessage {
    std::uint64_t sequence;
    std::uint64_t order_id;
    std::int64_t price;
    std::uint32_t instrument_id;
    std::uint32_t quantity;
    MessageType message_type;
    Side side;
};

inline constexpr std::int64_t kPriceScale = 10'000;
inline constexpr std::size_t kProtocolV1BufSize = 34;
using ProtocolV1Buffer = std::array<std::uint8_t, kProtocolV1BufSize>;
/*
    offset  size  field
    0       8     sequence
    8       8     order_id
    16      8     price
    24      4     instrument_id
    28      4     quantity
    32      1     message_type
    33      1     side

    Total payload size: 34 bytes.
*/
ProtocolV1Buffer encode_protocol_v1(const MarketDataMessage& message);
bool decode_protocol_v1(const std::uint8_t* data, std::size_t size, MarketDataMessage& message);

} // namespace udp_ingestion
