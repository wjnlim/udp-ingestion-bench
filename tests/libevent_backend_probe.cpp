#include <event2/event.h>

#include <cstring>
#include <iostream>

int main() {
    std::cout << "header_version=" << LIBEVENT_VERSION << '\n'
              << "runtime_version=" << event_get_version() << '\n';

    const char** methods = event_get_supported_methods();
    if (methods == nullptr) {
        std::cerr << "event_get_supported_methods failed\n";
        return 1;
    }

    std::cout << "supported_methods=";
    for (int i = 0; methods[i] != nullptr; ++i) {
        if (i != 0) {
            std::cout << ',';
        }
        std::cout << methods[i];
    }
    std::cout << '\n';

    event_base* base = event_base_new();
    if (base == nullptr) {
        std::cerr << "event_base_new failed\n";
        return 1;
    }

    const char* method = event_base_get_method(base);
    std::cout << "selected_backend=" << method << '\n';

    const bool uses_epoll = std::strcmp(method, "epoll") == 0;
    event_base_free(base);

    if (!uses_epoll) {
        std::cerr << "expected epoll backend for this comparison\n";
        return 1;
    }

    return 0;
}