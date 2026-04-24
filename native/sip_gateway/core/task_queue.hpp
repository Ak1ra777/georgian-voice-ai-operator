#pragma once

// Simple task queue used to keep PJSUA2 callbacks non-blocking.

#include <condition_variable>
#include <exception>
#include <functional>
#include <mutex>
#include <queue>

class TaskQueue {
  public:
    // Enqueue a task to be executed later.
    void post(std::function<void()> fn) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            tasks_.push(std::move(fn));
        }
        cv_.notify_one();
    }

    // Run all pending tasks without blocking.
    // If a task throws, keep draining and report via on_error.
    void drain(const std::function<void(std::exception_ptr)> &on_error = {}) {
        std::queue<std::function<void()>> local;
        {
            std::lock_guard<std::mutex> lock(mu_);
            std::swap(local, tasks_);
        }
        while (!local.empty()) {
            auto fn = std::move(local.front());
            local.pop();
            if (fn) {
                try {
                    fn();
                } catch (...) {
                    if (on_error) {
                        on_error(std::current_exception());
                    }
                }
            }
        }
    }

  private:
    std::mutex mu_;
    std::condition_variable cv_;
    std::queue<std::function<void()>> tasks_;
};
