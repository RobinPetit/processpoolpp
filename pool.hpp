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

class Buffer;

namespace impl {
template <typename T>
struct BufferImpl {
    static inline Buffer& out(Buffer&, T&);
    static inline Buffer& in(Buffer&, const T&);
};
}

class Buffer {
public:
    template <typename T> friend struct impl::BufferImpl;
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
        return impl::BufferImpl<T>::in(*this, obj);
    }

    template <typename T>
    inline Buffer& operator>>(T& obj) {
        return impl::BufferImpl<T>::out(*this, obj);
    }

    inline size_t size() const {
        return idx;
    }
private:
    size_t current_size;
    char* buffer;
    size_t idx;
};

namespace impl {
template <typename T>
Buffer& BufferImpl<T>::out(Buffer& buffer, T& obj) {
    static_assert(not std::is_pointer<T>::value, "Unable to deserialize pointers in general");
    static_assert(std::is_fundamental<T>::value, "Specialize operator>> for uses with Buffer");
    std::memcpy(&obj, buffer.buffer+buffer.idx, sizeof(obj));
    buffer.idx += sizeof(obj);
    return buffer;
}

template <typename T>
Buffer& BufferImpl<T>::in(Buffer& buffer, const T& obj) {
    static_assert(not std::is_pointer<T>::value, "Unable to serialize pointers in general");
    static_assert(std::is_fundamental<T>::value, "Specialize operator<< for uses with Buffer");
    buffer.need(sizeof(obj));
    std::memcpy(buffer.buffer+buffer.idx, &obj, sizeof(obj));
    buffer.idx += sizeof(obj);
    return buffer;
}

template <>
struct BufferImpl<std::string> {
    static inline Buffer& out(Buffer& buffer, std::string& string) {
        size_t len;
        buffer >> len;
        string.resize(len+1);
        for(size_t i{0}; i < len; ++i)
            buffer >> string.at(i);
        string[len] = '\0';
        return buffer;
    }

    static inline Buffer& in(Buffer& buffer, const std::string& string) {
        buffer.need(string.size()+sizeof(size_t));
        buffer << static_cast<size_t>(string.size());
        memcpy(buffer.buffer+buffer.idx, string.data(), string.size());
        buffer.idx += string.size();
        return buffer;
    }
};

template <typename T>
struct BufferImpl<std::vector<T>> {
    static inline Buffer& out(Buffer& buffer, std::vector<T>& obj) {
        obj.clear();
        size_t N;
        buffer >> N;
        obj.reserve(N);
        for(size_t i{0}; i < N; ++i) {
            T tmp;
            buffer >> tmp;
            obj.emplace_back(std::move(tmp));
        }
        return buffer;
    }
    static inline Buffer& in(Buffer& buffer, const std::vector<T>& obj) {
        buffer << obj.size();
        for(auto& x : obj)
            buffer << x;
        return buffer;
    }
};

template <typename Tuple, size_t Idx>
struct _TupleIterator {
    template <typename Caller, typename... Args>
    static void apply(Tuple& tuple, Args&... args) {
        if constexpr(Idx < std::tuple_size<Tuple>::value) {
            Caller::callback(std::get<Idx>(tuple), args...);
            _TupleIterator<Tuple, Idx+1>::template apply<Caller>(tuple, args...);
        }
    }
    template <typename Caller, typename... Args>
    static void apply(const Tuple& tuple, Args&... args) {
        if constexpr(Idx < std::tuple_size<Tuple>::value) {
            Caller::callback(std::get<Idx>(tuple), args...);
            _TupleIterator<Tuple, Idx+1>::template apply<Caller>(tuple, args...);
        }
    }
};

template <typename Tuple>
struct TupleIterator {
    template <typename Caller, typename... Args>
    static void apply(Tuple& tuple, Args&... args) {
        _TupleIterator<Tuple, static_cast<size_t>(0)>::template apply<Caller, Args...>(tuple, args...);
    }
    template <typename Caller, typename... Args>
    static void apply(const Tuple& tuple, Args&... args) {
        _TupleIterator<Tuple, static_cast<size_t>(0)>::template apply<Caller, Args...>(tuple, args...);
    }
};

struct OutCaller {
    template <typename T>
    static void callback(T& x, Buffer& buffer) {
        buffer >> x;
    }
};
struct InCaller {
    template <typename T>
    static void callback(const T& x, Buffer& buffer) {
        buffer << x;
    }
};
template <typename... Args>
struct BufferImpl<std::tuple<Args...>> {
    using Tuple = std::tuple<Args...>;
    static inline Buffer& out(Buffer& buffer, Tuple& t) {
        TupleIterator<Tuple>::template apply<OutCaller>(t, buffer);
        return buffer;
    }

    static inline Buffer& in(Buffer& buffer, const Tuple& t) {
        //TupleIterator<Tuple>::template apply<InCaller>(const_cast<std::tuple<Args...>&>(t), buffer);
        TupleIterator<Tuple>::template apply<InCaller>(t, buffer);
        return buffer;

    }
};

template <typename T, typename U>
struct BufferImpl<std::pair<T, U>> {
    static inline Buffer& out(Buffer& buffer, std::pair<T, U>& obj) {
        buffer >> obj.first;
        buffer >> obj.second;
        return buffer;
    }
    static inline Buffer& in(Buffer& buffer, const std::pair<T, U>& obj) {
        buffer << obj.first;
        buffer << obj.second;
        return buffer;
    }
};

}  // processpool::serialization::impl

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

template <typename T, typename U, typename C>
struct PoolGatheringOperation {
    using Container = std::remove_const_t<std::remove_reference_t<C>>;
    using Param = std::remove_const_t<std::remove_reference_t<U>>;
    using Type = std::remove_const_t<std::remove_reference_t<T>>;
    PoolGatheringOperation() {
        static_assert(std::is_same<typename C::value_type, T>::value);
    }
    virtual ~PoolGatheringOperation() = default;

    virtual void add_entry(const Type& result) = 0;
    virtual Container&& get() = 0;
};

template <typename T, typename U>
class DefaultPoolGatheringOperation : public PoolGatheringOperation<T, U, std::vector<T>> {
public:
    typedef PoolGatheringOperation<T, U, std::vector<T>> Base;
    typedef typename Base::Container Container;
    typedef typename Base::Param Param;
    typedef typename Base::Type Type;
public:
    DefaultPoolGatheringOperation():
            Base(), results() {
    }

    virtual ~DefaultPoolGatheringOperation() = default;

    virtual void add_entry(const Type& result) override {
        results.push_back(result);
    }

    virtual Container&& get() override {
        return std::move(results);
    }
private:
    std::vector<T> results;
};

template <typename Gatherer>
void __pool_gathering_info(Gatherer& result, const std::vector<WorkerDescriptor>& workers) {
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
            typename Gatherer::Type tmp;
            buffer >> tmp;
            result.add_entry(tmp);
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
        //ret.reserve(std::distance(beg, end));
        auto it{beg};
        while(it != end) {
            ret.push_back(f(*it));
            ++it;
        }
        return ret;
    }

    template <typename Iterator, typename T, typename U>
    inline std::vector<T> map_async(Iterator beg, Iterator end, T (*f)(U)) {
        return map_async<DefaultPoolGatheringOperation<T, U>>(beg, end, f);
    }

    template <typename Gatherer, typename Iterator, typename T, typename U>
    typename Gatherer::Container map_async(Iterator beg, Iterator end, T (*f)(U)) {
        allocate_workers(f);
        Gatherer ret;
        std::thread gathering_thread(__pool_gathering_info<Gatherer>, std::ref(ret), std::ref(workers));
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
        return ret.get();
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
            (void)error;
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
