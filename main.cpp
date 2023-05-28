#include "pool.hpp"

static int usage(const char* path) {
    std::cout << "Usage: " << path << " <nb_workers>" << std::endl;
    return 1;
}

int square(int x) { return x*x; }
std::string twice(const std::string& x) {
    return x + x;
}

using namespace processpool;

int main(int argc, const char* argv[]) {
    if(argc == 1) return usage(argv[0]);
    int nb_processes{std::stoi(argv[1])};
    ProcessPool pool(nb_processes);
    std::vector<int> v{{1, 2, 3, 4, 5, 6, 7}};
    for(auto s : pool.map_async(v.cbegin(), v.cend(), square))
        std::cout << s << std::endl;
    std::vector<std::string> w;
    char buffer[2]{};
    for(int i{0}; i < 10; ++i) {
        buffer[0] = i+'A';
        w.push_back(buffer);
    }
    for(auto x : pool.map_async(w.cbegin(), w.cend(), twice))
        std::cout << x << std::endl;
    return 0;
}
