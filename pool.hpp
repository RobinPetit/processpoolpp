#ifndef __PROCESS_POOL_HPP__
#define __PROCESS_POOL_HPP__

#include <errno.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/socket.h>

#include <cstring>

#include <iostream>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>
#include <string>
#include <string_view>
#include <thread>

namespace processpool {

enum class _IpcType {
    ACK,
    KILL,
    DATA,
    RESULT,
    EOT
};

struct _IpcHeader {
    _IpcType type;
    size_t length;
};

struct WorkerDescriptor {
    pid_t pid;
    int socket_fd;

    WorkerDescriptor() = default;
    WorkerDescriptor(pid_t p, int fd): pid{p}, socket_fd{fd} {}
};

namespace serialization {
class Buffer {
public:
    Buffer(size_t n): current_size{n}, buffer{nullptr}, idx{0} {
        buffer = new char[current_size];
        buffer[current_size] = '\0';
    }
    Buffer(): Buffer(1) {
    }

    ~Buffer() {
        delete[] buffer;
        buffer = nullptr;
    }

    void need(size_t size) {
        auto new_size{current_size};
        while(new_size-idx < size)
            new_size <<= 1;
        if(new_size != current_size) {
            char* tmp{new char[new_size]};
            std::memcpy(tmp, buffer, current_size);
            delete[] buffer;
            buffer = tmp;
            current_size = new_size;
        }
    }

    inline void reset() {
        idx = 0;
    }

    inline explicit operator char*() {
        return buffer;
    }

    template <typename T>
    inline Buffer& operator<<(const T*) {
        static_assert(std::is_pointer<T*>::value, "Unable to serialize pointers in general");
        return *this;
    }

    template <typename T>
    inline Buffer& operator<<(const T& obj) {
        static_assert(not std::is_pointer<T>::value, "Unable to serialize pointers in general");
        static_assert(std::is_fundamental<T>::value, "Specialize operator<< for uses with Buffer");
        need(sizeof(obj));
        std::memcpy(buffer+idx, &obj, sizeof(obj));
        idx += sizeof(obj);
        return *this;
    }

    template <typename T>
    inline Buffer& operator>>(T& obj) {
        static_assert(not std::is_pointer<T>::value, "Unable to deserialize pointers in general");
        static_assert(std::is_fundamental<T>::value, "Specialize operator>> for uses with Buffer");
        std::memcpy(&obj, buffer+idx, sizeof(obj));
        idx += sizeof(obj);
        return *this;
    }

    inline size_t size() const {
        return idx;
    }
private:
    size_t current_size;
    char* buffer;
    size_t idx;
};
template <>
inline Buffer& Buffer::operator<<(const std::string_view& obj) {
    need(obj.size()+sizeof(size_t));
    *this << static_cast<size_t>(obj.size());
    memcpy(buffer+idx, obj.data(), obj.size());
    idx += obj.size();
    return *this;
}
template <>
inline Buffer& Buffer::operator<<(const std::string& obj) {
    return *this << std::string_view(obj);
}
template <>
inline Buffer& Buffer::operator<<(const char* string) {
    return *this << std::string_view(string);
}

template <>
inline Buffer& Buffer::operator>>(std::string& string) {
    size_t len;
    *this >> len;
    string.resize(len+1);
    for(size_t i{0}; i < len; ++i)
        *this >> string.at(i);
    string[len] = '\0';
    return *this;
}

}

template <typename T, typename U>
class Worker {
public:
    Worker(int fd, T (*f)(U)): socket_fd{fd}, callback{f} {
    }
    void start() {
        bool running{true};
        _IpcHeader header;
        serialization::Buffer buffer;
        std::remove_const_t<std::remove_reference_t<U>> param;
        while(running) {
            buffer.reset();
            recv(socket_fd, static_cast<void*>(&header), sizeof(header), 0);
            switch(header.type) {
            case _IpcType::KILL:
                running = false;
                break;
            case _IpcType::DATA:
                buffer.need(header.length);
                recv(socket_fd, static_cast<char*>(buffer), header.length, 0);
                buffer >> param;
                result = callback(param);
                buffer.reset();
                buffer << result;
                header.type = _IpcType::RESULT;
                header.length = buffer.size();
                send(socket_fd, static_cast<void*>(&header), sizeof(header), 0);
                send(socket_fd, static_cast<char*>(buffer), buffer.size(), 0);
                break;
            case _IpcType::EOT:
                send(socket_fd, &header, sizeof(header), 0);
                break;
            default:
                break;
            }
        }
        close(socket_fd);
    }

private:
    int socket_fd;
    T result;
    T (*callback)(U);
};

template <typename T>
void __pool_gathering_info(std::vector<T>& result, const std::vector<WorkerDescriptor>& workers) {
    serialization::Buffer buffer;
    _IpcHeader header;
    std::vector<bool> is_running(workers.size(), true);
    int nb_running{static_cast<int>(workers.size())};
    bool loop{true};
    while(loop) {
        for(int i{0}; i < static_cast<int>(workers.size()); ++i) {
            auto [pid, fd] = workers[i];
            if(not is_running[i]) continue;
            auto error{recv(fd, static_cast<void*>(&header), sizeof(header), MSG_DONTWAIT)};
            if(error == -1) {
                if(errno == EAGAIN or errno == EWOULDBLOCK) continue;
                else std::cerr << "Unexpected errno: " << errno << std::endl;
            }
            if(header.type == _IpcType::EOT) {
                is_running[i] = false;
                if(--nb_running == 0)
                    loop = false;
                continue;
            } else if(header.type != _IpcType::RESULT) {
                std::cerr << "We got a problem\n";
                return;
            }
            buffer.reset();
            buffer.need(header.length);
            error = recv(fd, static_cast<char*>(buffer), header.length, 0);
            T tmp;
            buffer >> tmp;
            result.push_back(tmp);
        }
    }
}

class ProcessPool {
public:
    ProcessPool(int n):
            nb_workers(n), workers() {
    }

    ~ProcessPool() {
        close();
    }

    void close() {
        int status;  // ignored
        for(auto worker : workers) {
            _IpcHeader header{_IpcType::KILL, 0};
            send(worker.socket_fd, &header, sizeof(header), 0);
            // specify the namespace to prevent compiling error
            ::close(worker.socket_fd);
            waitpid(worker.pid, &status, 0);
        }
        workers.clear();
    }

    template <typename Iterator, typename T, typename U>
    std::vector<T> map(Iterator beg, Iterator end, T(*f)(U)) {
        std::vector<T> ret;
        ret.reserve(std::distance(beg, end));
        auto it{beg};
        while(it != end) {
            ret.push_back(f(*it));
            ++it;
        }
        return ret;
    }

    template <typename Iterator, typename T, typename U>
    std::vector<T> map_async(Iterator beg, Iterator end, T (*f)(U)) {
        allocate_workers(f);
        std::vector<T> ret;
        std::thread gathering_thread(__pool_gathering_info<T>, std::ref(ret), std::ref(workers));
        auto it{beg};
        int counter{0};
        serialization::Buffer buff;
        while(it != end) {
            auto fd{workers[counter].socket_fd};
            const auto& data{*it};
            buff.reset();
            buff << data;
            _IpcHeader header{_IpcType::DATA, buff.size()};
            send(fd, &header, sizeof(header), 0);
            send(fd, static_cast<char*>(buff), buff.size(), 0);
            ++it;
            counter = (counter+1) % nb_workers;
        }
        _IpcHeader header{_IpcType::EOT, 0};
        for(auto [pid, fd] : workers)
            send(fd, &header, sizeof(header), 0);
        gathering_thread.join();
        return ret;
    }
private:
    int nb_workers;
    std::vector<WorkerDescriptor> workers;

    template <typename T, typename U>
    void allocate_workers(T (*callback)(U)) {
        if(nb_workers <= 0) {
            throw new std::runtime_error("There must be at least one process in the pool!");
        }
        close();
        bool is_main_process{true};
        for(int i{0}; i < nb_workers; ++i) {
            int sockets[2];
            int error{socketpair(AF_LOCAL, SOCK_STREAM, 0, sockets)};
            // TODO: check error
            pid_t pid{fork()};
            if(pid == 0) {
                Worker(sockets[0], callback).start();
                exit(EXIT_SUCCESS);
            } else {
                workers.emplace_back(pid, sockets[1]);
            }
        }
        if(not is_main_process)
            workers.clear();
    }
};

}  // processpool

# endif