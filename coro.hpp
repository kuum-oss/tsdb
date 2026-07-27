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

        Task get_return_object() {
            return Task(std::coroutine_handle<promise_type>::from_promise(*this));
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
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
        void await_suspend(std::coroutine_handle<> calling) noexcept {
            handle.resume();
            calling.resume();
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

        Task get_return_object() {
            return Task(std::coroutine_handle<promise_type>::from_promise(*this));
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_always final_suspend() noexcept { return {}; }
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
        void await_suspend(std::coroutine_handle<> calling) noexcept {
            handle.resume();
            calling.resume();
        }
        void await_resume() {
            if (handle.promise().exception) {
                std::rethrow_exception(handle.promise().exception);
            }
        }
    };

    auto operator co_await() { return Awaiter{handle}; }
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
            std::thread([this, h]() mutable {
                fut.wait();
                h.resume();
            }).detach();
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
        std::future<T> fut;
        bool await_ready() const noexcept {
            return fut.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
        }
        void await_suspend(std::coroutine_handle<> h) {
            std::thread([this, h]() mutable {
                fut.wait();
                h.resume();
            }).detach();
        }
        T await_resume() {
            return fut.get();
        }
    };
    return Awaiter{std::move(fut)};
}
