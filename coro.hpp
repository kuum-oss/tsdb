#pragma once
#include <coroutine>
#include <exception>
#include <future>
#include <utility>
#include <chrono>
#include <thread>

template<typename T>
struct Task {
    struct promise_type {
        T value;
        std::exception_ptr exception;
        std::coroutine_handle<> continuation;

        Task get_return_object() {
            return Task(std::coroutine_handle<promise_type>::from_promise(*this));
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        struct FinalAwaiter {
            bool await_ready() const noexcept { return false; }
            std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                if (h.promise().continuation) {
                    return h.promise().continuation;
                }
                return std::noop_coroutine();
            }
            void await_resume() noexcept {}
        };
        FinalAwaiter final_suspend() noexcept { return {}; }
        void return_value(T val) { value = std::move(val); }
        void unhandled_exception() { exception = std::current_exception(); }
    };

    std::coroutine_handle<promise_type> handle;

    Task(std::coroutine_handle<promise_type> h) : handle(h) {}
    ~Task() { if (handle) handle.destroy(); }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    Task(Task&& other) noexcept : handle(other.handle) { other.handle = nullptr; }
    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle) handle.destroy();
            handle = other.handle;
            other.handle = nullptr;
        }
        return *this;
    }

    struct Awaiter {
        std::coroutine_handle<promise_type> handle;
        bool await_ready() const noexcept { return false; }
        std::coroutine_handle<> await_suspend(std::coroutine_handle<> calling) noexcept {
            // Store the calling coroutine to resume it when this task finishes.
            // Using C++20 symmetric transfer.
            // To do this, we need to handle continuation. Let's design it simply:
            // Since we want true async, we should resume the child task, and when the child finishes,
            // resume the caller.
            // Let's implement continuation in promise_type.
            handle.promise().continuation = calling;
            return handle;
        }
        T await_resume() {
            if (handle.promise().exception) {
                std::rethrow_exception(handle.promise().exception);
            }
            return std::move(handle.promise().value);
        }
    };

    auto operator co_await() { return Awaiter{handle}; }
};

template<>
struct Task<void> {
    struct promise_type {
        std::exception_ptr exception;
        std::coroutine_handle<> continuation;

        Task get_return_object() {
            return Task(std::coroutine_handle<promise_type>::from_promise(*this));
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        struct FinalAwaiter {
            bool await_ready() const noexcept { return false; }
            std::coroutine_handle<> await_suspend(std::coroutine_handle<promise_type> h) noexcept {
                if (h.promise().continuation) {
                    return h.promise().continuation;
                }
                return std::noop_coroutine();
            }
            void await_resume() noexcept {}
        };
        FinalAwaiter final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { exception = std::current_exception(); }
    };

    std::coroutine_handle<promise_type> handle;

    Task(std::coroutine_handle<promise_type> h) : handle(h) {}
    ~Task() { if (handle) handle.destroy(); }
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;
    Task(Task&& other) noexcept : handle(other.handle) { other.handle = nullptr; }
    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (handle) handle.destroy();
            handle = other.handle;
            other.handle = nullptr;
        }
        return *this;
    }

    struct Awaiter {
        std::coroutine_handle<promise_type> handle;
        bool await_ready() const noexcept { return false; }
        std::coroutine_handle<> await_suspend(std::coroutine_handle<> calling) noexcept {
            handle.promise().continuation = calling;
            return handle;
        }
        void await_resume() {
            if (handle.promise().exception) {
                std::rethrow_exception(handle.promise().exception);
            }
        }
    };

    auto operator co_await() { return Awaiter{handle}; }
};

#include <queue>
#include <mutex>
#include <condition_variable>

class FutureWaiter {
private:
    struct WaitItem {
        std::function<bool()> is_ready;
        std::coroutine_handle<> handle;
    };
    std::vector<WaitItem> items;
    std::mutex mtx;
    std::condition_variable cv;
    std::thread worker;
    std::atomic<bool> running{true};

    FutureWaiter() {
        worker = std::thread([this]() {
            while (running) {
                std::vector<std::coroutine_handle<>> ready_handles;
                {
                    std::unique_lock lock(mtx);
                    cv.wait_for(lock, std::chrono::milliseconds(10), [this]() {
                        return !items.empty() || !running;
                    });
                    if (!running) break;
                    for (auto it = items.begin(); it != items.end(); ) {
                        if (it->is_ready()) {
                            ready_handles.push_back(it->handle);
                            it = items.erase(it);
                        } else {
                            ++it;
                        }
                    }
                }
                for (auto h : ready_handles) {
                    h.resume();
                }
            }
        });
    }

public:
    ~FutureWaiter() {
        running = false;
        cv.notify_all();
        if (worker.joinable()) {
            worker.join();
        }
    }

    static FutureWaiter& instance() {
        static FutureWaiter inst;
        return inst;
    }

    void add(std::function<bool()> is_ready, std::coroutine_handle<> h) {
        {
            std::unique_lock lock(mtx);
            items.push_back({std::move(is_ready), h});
        }
        cv.notify_one();
    }
};

// Operator co_await on std::shared_future
template<typename T>
auto operator co_await(std::shared_future<T> fut) {
    struct Awaiter {
        std::shared_future<T> fut;
        bool await_ready() const noexcept {
            return fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
        }
        void await_suspend(std::coroutine_handle<> h) {
            auto shared_fut = fut;
            FutureWaiter::instance().add([shared_fut]() {
                return shared_fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
            }, h);
        }
        T await_resume() {
            return fut.get();
        }
    };
    return Awaiter{std::move(fut)};
}

// Operator co_await on std::future
template<typename T>
auto operator co_await(std::future<T> fut) {
    struct Awaiter {
        std::shared_future<T> fut; // Convert to shared_future to allow capturing in lambda
        bool await_ready() const noexcept {
            return fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
        }
        void await_suspend(std::coroutine_handle<> h) {
            auto shared_fut = fut;
            FutureWaiter::instance().add([shared_fut]() {
                return shared_fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
            }, h);
        }
        T await_resume() {
            return fut.get();
        }
    };
    return Awaiter{std::move(fut).share()};
}
