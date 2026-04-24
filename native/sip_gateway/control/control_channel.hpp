#pragma once

// Control channel client:
// - Connects to a Unix socket exposed by Python.
// - Sends JSON events (call_allocated, call_media_started, call_end, control_ack).
// - Receives JSON lines (call_ready, call_hangup) and invokes callbacks.

#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

class ControlChannel {
  public:
    using ReadyCallback = std::function<void(const std::string &session_id)>;
    using HangupCallback =
        std::function<void(const std::string &session_id,
                           const std::string &reason)>;

    // socket_path: Unix socket path where Python listens.
    // on_ready: callback invoked when a call_ready message is received.
    // on_hangup: callback invoked when Python requests call hangup.
    ControlChannel(std::string socket_path,
                   ReadyCallback on_ready,
                   HangupCallback on_hangup = HangupCallback());
    ~ControlChannel();

    // Connect to the socket and start the reader thread.
    void start();
    // Stop the reader thread and close the socket.
    void stop();

    // Send a JSON line event to Python.
    void send_event(const std::string &json_line);

  private:
    // Read loop: parses newline-delimited JSON messages.
    void read_loop();
    // Establish the Unix domain socket connection.
    // write_mu_ must be held by caller.
    bool connect_socket_locked();

    // Unix socket path to Python control server.
    std::string socket_path_;
    // Callback invoked when Python signals readiness.
    ReadyCallback on_ready_;
    // Callback invoked when Python requests hangup.
    HangupCallback on_hangup_;
    // Connected socket file descriptor.
    int sock_{-1};
    // Reader thread lifecycle.
    std::atomic<bool> running_{false};
    std::thread reader_;
    // Serialize writes to the socket.
    std::mutex write_mu_;
};
