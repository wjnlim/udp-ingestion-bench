#pragma once

namespace udp_ingestion {

// stop notification for an epoll loop
class StopEvent {
public:
    StopEvent();
    ~StopEvent();

    StopEvent(const StopEvent&) = delete;
    StopEvent& operator=(const StopEvent&) = delete;

    int get_fd() const noexcept {
        return fd_;
    }

    void notify();
private:
    int fd_ = -1;
};

} // namespace udp_ingestion