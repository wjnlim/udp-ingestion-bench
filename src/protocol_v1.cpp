#include "udp_ingestion/protocol_v1.hpp"

#include <limits>

namespace udp_ingestion {
namespace {
// write a u64 in Big-endian order
void write_u64(ProtocolV1Buffer& buffer, std::size_t offset, std::uint64_t value) {
    for (std::size_t i = 0; i < 8; ++i) {
        buffer[offset + i] =
            static_cast<std::uint8_t>(value >> ((7 - i) * 8));
    }
}
// write a u32 in Big-endian order
void write_u32(ProtocolV1Buffer& buffer, std::size_t offset, std::uint32_t value) {
    for (std::size_t i = 0; i < 4; ++i) {
        buffer[offset + i] =
            static_cast<std::uint8_t>(value >> ((3 - i) * 8));
    }
}

std::uint64_t read_u64(const std::uint8_t* data, std::size_t offset) {
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < 8; ++i) {
        value = (value << 8) | data[offset + i];
    }
    return value;
}

std::uint32_t read_u32(const std::uint8_t* data, std::size_t offset) {
    std::uint32_t value = 0;
    for (std::size_t i = 0; i < 4; ++i) {
        value = (value << 8) | data[offset + i];
    }
    return value;
}

// convert a signed int64_t into its 2's complement representation 
// regardless of underlying binary representation of negative integer
std::uint64_t signed_to_twos_complement(std::int64_t value) {
    if (value >= 0) {
        return static_cast<std::uint64_t>(value);
    }
    /*
        The two's complement of x is defined as the number that, 
        when added to x, results in 2^n. 
        To obtain the two's complement representation (tc) of a negative number v, 
        we take the two's complement of its absolute value. 
        Mathematically, this is 'tc = 2^64 - (-v)'. 
        However, since -v can cause an overflow, 
        the formula can be rewritten by adding and subtracting 1 to prevent this, 
        resulting in: tc = 2^64 - (-(v+1-1)) => tc = 2^64 - 1 - (-(v+1))
    */
    return std::numeric_limits<std::uint64_t>::max() -
           static_cast<std::uint64_t>(-(value + 1));
}

std::int64_t twos_complement_to_signed(std::uint64_t value) {
    const auto signed_max =
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
    if (value <= signed_max) {
        return static_cast<std::int64_t>(value);
    }
    return -1 - static_cast<std::int64_t>(
                    std::numeric_limits<std::uint64_t>::max() - value);
}

bool valid_message_type(std::uint8_t value) {
    return value <= static_cast<std::uint8_t>(MessageType::Trade);
}

bool valid_side(std::uint8_t value) {
    return value <= static_cast<std::uint8_t>(Side::Sell);
}

}  // namespace

ProtocolV1Buffer encode_protocol_v1(const MarketDataMessage& message) {
    ProtocolV1Buffer buffer{};
    write_u64(buffer, 0, message.sequence);
    write_u64(buffer, 8, message.order_id);
    write_u64(buffer, 16, signed_to_twos_complement(message.price));
    write_u32(buffer, 24, message.instrument_id);
    write_u32(buffer, 28, message.quantity);
    buffer[32] = static_cast<std::uint8_t>(message.message_type);
    buffer[33] = static_cast<std::uint8_t>(message.side);
    return buffer;
}

bool decode_protocol_v1(const std::uint8_t* data,
                        std::size_t size,
                        MarketDataMessage& message) {
    if (data == nullptr || size != kProtocolV1BufSize ||
        !valid_message_type(data[32]) || !valid_side(data[33])) {
        return false;
    }

    message.sequence = read_u64(data, 0);
    message.order_id = read_u64(data, 8);
    message.price = twos_complement_to_signed(read_u64(data, 16));
    message.instrument_id = read_u32(data, 24);
    message.quantity = read_u32(data, 28);
    message.message_type = static_cast<MessageType>(data[32]);
    message.side = static_cast<Side>(data[33]);
    return true;
}

}  // namespace udp_ingestion
