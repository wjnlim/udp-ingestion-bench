#pragma once

#include <string>

namespace udp_ingestion {

// Pins the calling thread to one Linux logical CPU.
// Throws on failure; role is used only for error reporting.
void pin_current_thread(int cpu, const std::string& role);

} // namespace udp_ingestion
