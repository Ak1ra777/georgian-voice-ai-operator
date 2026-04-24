// Implementation of the per-call PCM UDP bridge.
#include "media/udp_audio_port.hpp"

#include <pjmedia/format.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <vector>

using namespace pj;

// Fixed-size byte ring buffer used for jitter smoothing.
class UdpAudioPort::ByteRingBuffer {
  public:
    enum class OverflowPolicy { DropNewest, DropOldest };

    explicit ByteRingBuffer(size_t capacity_bytes)
        : buffer_(capacity_bytes),
          head_(0),
          tail_(0),
          size_(0) {}

    // Write bytes into the ring buffer with overflow policy.
    size_t write(const uint8_t *data,
                 size_t bytes,
                 OverflowPolicy policy,
                 size_t *dropped_bytes = nullptr) {
        if (bytes == 0 || buffer_.empty()) {
            if (dropped_bytes != nullptr) {
                *dropped_bytes = 0;
            }
            return 0;
        }
        std::lock_guard<std::mutex> lock(mu_);
        size_t dropped = 0;

        if (bytes > buffer_.size()) {
            data += (bytes - buffer_.size());
            dropped += (bytes - buffer_.size());
            bytes = buffer_.size();
        }

        size_t space = buffer_.size() - size_;
        if (bytes > space) {
            if (policy == OverflowPolicy::DropOldest) {
                size_t drop = bytes - space;
                tail_ = (tail_ + drop) % buffer_.size();
                size_ -= drop;
                dropped += drop;
            } else {
                dropped += (bytes - space);
                bytes = space;
            }
        }

        if (bytes == 0) {
            if (dropped_bytes != nullptr) {
                *dropped_bytes = dropped;
            }
            return 0;
        }

        size_t first = std::min(bytes, buffer_.size() - head_);
        std::memcpy(&buffer_[head_], data, first);
        size_t remaining = bytes - first;
        if (remaining > 0) {
            std::memcpy(&buffer_[0], data + first, remaining);
        }
        head_ = (head_ + bytes) % buffer_.size();
        size_ += bytes;
        if (dropped_bytes != nullptr) {
            *dropped_bytes = dropped;
        }
        return bytes;
    }

    // Read up to "bytes" from the buffer into out; returns bytes read.
    size_t read(uint8_t *out, size_t bytes) {
        if (bytes == 0 || buffer_.empty()) {
            return 0;
        }
        std::lock_guard<std::mutex> lock(mu_);
        size_t to_read = std::min(bytes, size_);
        if (to_read == 0) {
            return 0;
        }
        size_t first = std::min(to_read, buffer_.size() - tail_);
        std::memcpy(out, &buffer_[tail_], first);
        size_t remaining = to_read - first;
        if (remaining > 0) {
            std::memcpy(out + first, &buffer_[0], remaining);
        }
        tail_ = (tail_ + to_read) % buffer_.size();
        size_ -= to_read;
        return to_read;
    }

    // Current buffered size (bytes).
    size_t size() const {
        std::lock_guard<std::mutex> lock(mu_);
        return size_;
    }

  private:
    mutable std::mutex mu_;
    std::vector<uint8_t> buffer_;
    size_t head_;
    size_t tail_;
    size_t size_;
};

namespace {
// Call-side audio format (PJSUA2) is fixed at 8kHz mono PCM16.
constexpr unsigned kSampleRate8k = 8000;
// Python-side UDP bridge uses 16kHz mono PCM16.
constexpr unsigned kSampleRate16k = 16000;
constexpr unsigned kChannels = 1;
// Frame size is 20ms (common RTP packetization interval).
constexpr unsigned kFrameTimeUsec = 20000;
constexpr unsigned kSamplesPerFrame8k =
    (kSampleRate8k * kFrameTimeUsec) / 1000000;
constexpr unsigned kSamplesPerFrame16k =
    (kSampleRate16k * kFrameTimeUsec) / 1000000;
constexpr size_t kFrameBytes8k = kSamplesPerFrame8k * sizeof(int16_t);
constexpr size_t kFrameBytes16k = kSamplesPerFrame16k * sizeof(int16_t);

// Simple linear upsampler (8k -> 16k).
// This is cheap and adequate for telephony voice.
void Upsample2x(const int16_t *in, size_t in_samples, int16_t *out) {
    for (size_t i = 0; i < in_samples; ++i) {
        int16_t cur = in[i];
        int16_t next = (i + 1 < in_samples) ? in[i + 1] : cur;
        out[2 * i] = cur;
        out[2 * i + 1] = static_cast<int16_t>(
            (static_cast<int32_t>(cur) + static_cast<int32_t>(next)) / 2);
    }
}

// Simple averaging downsampler (16k -> 8k).
void Downsample2x(const int16_t *in, size_t in_samples, int16_t *out) {
    size_t out_samples = in_samples / 2;
    for (size_t i = 0; i < out_samples; ++i) {
        int32_t a = in[2 * i];
        int32_t b = in[2 * i + 1];
        out[i] = static_cast<int16_t>((a + b) / 2);
    }
}

void UpdateHighWatermark(std::atomic<uint64_t> &watermark, size_t value) {
    uint64_t observed = watermark.load(std::memory_order_relaxed);
    const uint64_t candidate = static_cast<uint64_t>(value);
    while (candidate > observed &&
           !watermark.compare_exchange_weak(
               observed,
               candidate,
               std::memory_order_relaxed,
               std::memory_order_relaxed)) {
    }
}
}  // namespace

// Store config, set up ring buffers, and keep any pre-bound recv socket.
UdpAudioPort::UdpAudioPort(const AudioConfig &cfg, int reserved_recv_sock)
    : cfg_(cfg),
      reserved_recv_sock_(reserved_recv_sock),
      rx_ring_(std::make_unique<ByteRingBuffer>(cfg.ring_packets * kFrameBytes16k)),
      tx_ring_(std::make_unique<ByteRingBuffer>(cfg.ring_packets * kFrameBytes16k)),
      sock_send_(-1),
      sock_recv_(-1),
      ring_capacity_bytes_(cfg.ring_packets * kFrameBytes16k) {}

// Ensure threads/sockets are stopped when the port is destroyed.
UdpAudioPort::~UdpAudioPort() {
    stop();
}

// Create the PJMEDIA port, bind sockets, and start RX/TX threads.
void UdpAudioPort::start() {
    MediaFormatAudio fmt;
    fmt.init(PJMEDIA_FORMAT_L16,
             kSampleRate8k,
             kChannels,
             kFrameTimeUsec,
             16);
    createPort("udp_audio_port", fmt);

    sock_send_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_send_ < 0) {
        throw std::runtime_error("Failed to create UDP send socket");
    }

    // If a pre-bound socket was reserved by PortAllocator, reuse it.
    if (reserved_recv_sock_ >= 0) {
        sock_recv_ = reserved_recv_sock_;
        reserved_recv_sock_ = -1;
    } else {
        sock_recv_ = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (sock_recv_ < 0) {
            throw std::runtime_error("Failed to create UDP receive socket");
        }

        // Allow quick restarts if the port is still in TIME_WAIT.
        int reuse = 1;
        setsockopt(sock_recv_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

        sockaddr_in recv_addr{};
        recv_addr.sin_family = AF_INET;
        recv_addr.sin_port = htons(cfg_.tx_port);
        recv_addr.sin_addr.s_addr = INADDR_ANY;
        if (bind(sock_recv_, reinterpret_cast<sockaddr *>(&recv_addr),
                 sizeof(recv_addr)) != 0) {
            throw std::runtime_error("Failed to bind UDP receive socket");
        }
    }

    std::memset(&send_addr_, 0, sizeof(send_addr_));
    send_addr_.sin_family = AF_INET;
    send_addr_.sin_port = htons(cfg_.rx_port);
    if (inet_pton(AF_INET, cfg_.host.c_str(), &send_addr_.sin_addr) != 1) {
        throw std::runtime_error("Invalid SIP_AUDIO_HOST");
    }

    // Short timeout so recv loop can check stop flag.
    timeval timeout{};
    timeout.tv_sec = 0;
    timeout.tv_usec = 20000;
    setsockopt(sock_recv_, SOL_SOCKET, SO_RCVTIMEO, &timeout,
               sizeof(timeout));

    // Start background threads for sending and receiving.
    running_.store(true);
    rx_thread_ = std::thread(&UdpAudioPort::rxSenderLoop, this);
    tx_thread_ = std::thread(&UdpAudioPort::txReceiverLoop, this);
}

// Stop threads and close sockets.
void UdpAudioPort::stop() {
    running_.store(false);
    if (sock_recv_ >= 0) {
        shutdown(sock_recv_, SHUT_RDWR);
    }
    if (rx_thread_.joinable()) {
        rx_thread_.join();
    }
    if (tx_thread_.joinable()) {
        tx_thread_.join();
    }
    if (sock_send_ >= 0) {
        close(sock_send_);
        sock_send_ = -1;
    }
    if (sock_recv_ >= 0) {
        close(sock_recv_);
        sock_recv_ = -1;
    }
}

UdpAudioPort::BridgeStats UdpAudioPort::stats_snapshot() const {
    BridgeStats stats;
    stats.rx_udp_packets_sent = rx_udp_packets_sent_.load(std::memory_order_relaxed);
    stats.rx_udp_bytes_sent = rx_udp_bytes_sent_.load(std::memory_order_relaxed);
    stats.rx_udp_send_errors = rx_udp_send_errors_.load(std::memory_order_relaxed);
    stats.rx_ring_bytes_written = rx_ring_bytes_written_.load(std::memory_order_relaxed);
    stats.rx_ring_bytes_dropped = rx_ring_bytes_dropped_.load(std::memory_order_relaxed);
    stats.rx_ring_high_watermark_bytes =
        rx_ring_high_watermark_bytes_.load(std::memory_order_relaxed);

    stats.tx_udp_packets_received =
        tx_udp_packets_received_.load(std::memory_order_relaxed);
    stats.tx_udp_bytes_received = tx_udp_bytes_received_.load(std::memory_order_relaxed);
    stats.tx_udp_recv_errors = tx_udp_recv_errors_.load(std::memory_order_relaxed);
    stats.tx_ring_bytes_written = tx_ring_bytes_written_.load(std::memory_order_relaxed);
    stats.tx_ring_bytes_dropped = tx_ring_bytes_dropped_.load(std::memory_order_relaxed);
    stats.tx_ring_high_watermark_bytes =
        tx_ring_high_watermark_bytes_.load(std::memory_order_relaxed);

    stats.tx_underrun_frames = tx_underrun_frames_.load(std::memory_order_relaxed);
    stats.tx_underrun_bytes = tx_underrun_bytes_.load(std::memory_order_relaxed);
    stats.ring_capacity_bytes = static_cast<uint64_t>(ring_capacity_bytes_);
    return stats;
}

// Called by PJSUA2 when new audio arrives from the call (8 kHz).
// We upsample to 16 kHz and enqueue for UDP send.
void UdpAudioPort::onFrameReceived(MediaFrame &frame) {
    if (frame.type != PJMEDIA_FRAME_TYPE_AUDIO ||
        frame.size != kFrameBytes8k) {
        return;
    }
    // Convert 8 kHz PCM16 -> 16 kHz PCM16.
    const int16_t *in =
        reinterpret_cast<const int16_t *>(frame.buf.data());
    int16_t out[kSamplesPerFrame16k];
    Upsample2x(in, kSamplesPerFrame8k, out);
    // Buffer for UDP sending (drop newest on overflow).
    size_t dropped = 0;
    size_t written = rx_ring_->write(reinterpret_cast<const uint8_t *>(out),
                                     kFrameBytes16k,
                                     ByteRingBuffer::OverflowPolicy::DropNewest,
                                     &dropped);
    rx_ring_bytes_written_.fetch_add(written, std::memory_order_relaxed);
    rx_ring_bytes_dropped_.fetch_add(dropped, std::memory_order_relaxed);
    UpdateHighWatermark(rx_ring_high_watermark_bytes_, rx_ring_->size());
}

// Called by PJSUA2 when it needs audio to play to the call (8 kHz).
// We dequeue 16 kHz audio from UDP, downsample, and return a frame.
void UdpAudioPort::onFrameRequested(MediaFrame &frame) {
    frame.type = PJMEDIA_FRAME_TYPE_AUDIO;
    frame.size = kFrameBytes8k;
    frame.buf.resize(kFrameBytes8k);

    // Pull the next 16 kHz frame from the UDP buffer.
    int16_t in[kSamplesPerFrame16k];
    size_t got = tx_ring_->read(reinterpret_cast<uint8_t *>(in),
                                kFrameBytes16k);
    if (got < kFrameBytes16k) {
        // Pad with silence if we are underrunning.
        tx_underrun_frames_.fetch_add(1, std::memory_order_relaxed);
        tx_underrun_bytes_.fetch_add(
            static_cast<uint64_t>(kFrameBytes16k - got),
            std::memory_order_relaxed);
        std::memset(reinterpret_cast<uint8_t *>(in) + got,
                    0,
                    kFrameBytes16k - got);
    }

    // Downsample 16 kHz -> 8 kHz and return to PJSUA2.
    int16_t out[kSamplesPerFrame8k];
    Downsample2x(in, kSamplesPerFrame16k, out);
    std::memcpy(frame.buf.data(), out, kFrameBytes8k);
}

// Worker thread: send buffered 16k frames to Python via UDP.
void UdpAudioPort::rxSenderLoop() {
    std::vector<uint8_t> packet(kFrameBytes16k);
    while (running_.load()) {
        if (rx_ring_->size() < kFrameBytes16k) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        // Read one 20ms frame and send it over UDP.
        size_t got = rx_ring_->read(packet.data(), packet.size());
        if (got < packet.size()) {
            std::memset(packet.data() + got, 0, packet.size() - got);
        }
        ssize_t sent = sendto(sock_send_,
                              packet.data(),
                              packet.size(),
                              0,
                              reinterpret_cast<sockaddr *>(&send_addr_),
                              sizeof(send_addr_));
        if (sent < 0) {
            rx_udp_send_errors_.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        rx_udp_packets_sent_.fetch_add(1, std::memory_order_relaxed);
        rx_udp_bytes_sent_.fetch_add(
            static_cast<uint64_t>(sent),
            std::memory_order_relaxed);
    }
}

// Worker thread: receive 16k frames from Python via UDP.
void UdpAudioPort::txReceiverLoop() {
    std::vector<uint8_t> packet(kFrameBytes16k);
    while (running_.load()) {
        // Read one 20ms packet from Python (non-blocking with timeout).
        ssize_t received =
            recvfrom(sock_recv_,
                     packet.data(),
                     packet.size(),
                     0,
                     nullptr,
                     nullptr);
        if (received <= 0) {
            if (received < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
                tx_udp_recv_errors_.fetch_add(1, std::memory_order_relaxed);
            }
            continue;
        }
        tx_udp_packets_received_.fetch_add(1, std::memory_order_relaxed);
        tx_udp_bytes_received_.fetch_add(
            static_cast<uint64_t>(received),
            std::memory_order_relaxed);
        // Buffer for playback; drop oldest on overflow to keep latency bounded.
        size_t dropped = 0;
        size_t written = tx_ring_->write(packet.data(),
                                         static_cast<size_t>(received),
                                         ByteRingBuffer::OverflowPolicy::DropOldest,
                                         &dropped);
        tx_ring_bytes_written_.fetch_add(written, std::memory_order_relaxed);
        tx_ring_bytes_dropped_.fetch_add(dropped, std::memory_order_relaxed);
        UpdateHighWatermark(tx_ring_high_watermark_bytes_, tx_ring_->size());
    }
}
