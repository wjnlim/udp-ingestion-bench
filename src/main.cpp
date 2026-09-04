#include <iostream>
#include <thread>

int main()
{
    int worker_result = 0;
    std::thread worker([&worker_result] { worker_result = 17; });
    worker.join();

    std::cout << "C++" << worker_result << " thread support verified\n";
    return 0;
}
