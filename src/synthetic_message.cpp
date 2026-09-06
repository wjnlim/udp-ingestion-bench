#include "udp_ingestion/synthetic_message.hpp"

#include <array>
#include <stdexcept>

namespace udp_ingestion {
    
// produce a deterministic syntheic market data base on arguments
MarketDataMessage make_synthetic_message(std::size_t channel,
                                         std::uint64_t sequence,
                                         std::uint32_t instrument_count) {
    if (instrument_count == 0) {
        throw std::invalid_argument("instrument_count must be positive");
    }

    constexpr std::array<MessageType, 4> message_types{
        MessageType::Add,
        MessageType::Modify,
        MessageType::Cancel,
        MessageType::Trade,
    };
    const auto instrument_id = static_cast<std::uint32_t>(
                                   (sequence - 1 + channel) % instrument_count) + 1;

    return MarketDataMessage{
        sequence,
        (static_cast<std::uint64_t>(channel + 1) << 56) | sequence,
        100 * kPriceScale + static_cast<std::int64_t>(instrument_id) * 25,
        instrument_id,
        static_cast<std::uint32_t>(100 + (sequence % 900)),
        message_types[(sequence - 1 + channel) % message_types.size()],
        ((sequence + channel) % 2 == 0) ? Side::Buy : Side::Sell,
    };
}

}  // namespace udp_ingestion
