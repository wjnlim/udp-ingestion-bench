#pragma once

#include "udp_ingestion/protocol_v1.hpp"

#include <cstddef>
#include <cstdint>

namespace udp_ingestion {

MarketDataMessage make_synthetic_message(std::size_t channel,
                                         std::uint64_t sequence,
                                         std::uint32_t instrument_count);

}  // namespace udp_ingestion
