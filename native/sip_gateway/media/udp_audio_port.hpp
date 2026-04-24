#pragma once

// Audio media port that bridges SIP audio to Python over raw UDP PCM.
// - Call side is 8 kHz PCM16.
// - UDP side is 16 kHz PCM16.

#include <pjsua2.hpp>
#include <netinet/in.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

struct AudioConfig {
    // Destination host for UDP packets (typically 127.0.0.1).
    std::string host;
    // Port used to send audio to Python (gateway -> Python).
    uint16_t rx_port;
    // Port used to receive audio from Python (Python -> gateway).
    uint16_t tx_port;
    // Ring buffer size in packets (jitter tolerance).
    size_t ring_packets;
};

class UdpAudioPort : public pj::AudioMediaPort {
  public:
    struct BridgeStats {
        uint64_t rx_udp_packets_sent{0};
        uint64_t rx_udp_bytes_sent{0};
        uint64_t rx_udp_send_errors{0};
        uint64_t rx_ring_bytes_written{0};
        uint64_t rx_ring_bytes_dropped{0};
        uint64_t rx_ring_high_watermark_bytes{0};

        uint64_t tx_udp_packets_received{0};
        uint64_t tx_udp_bytes_received{0};
        uint64_t tx_udp_recv_errors{0};
        uint64_t tx_ring_bytes_written{0};
        uint64_t tx_ring_bytes_dropped{0};
        uint64_t tx_ring_high_watermark_bytes{0};

        uint64_t tx_underrun_frames{0};
        uint64_t tx_underrun_bytes{0};

        uint64_t ring_capacity_bytes{0};
    };

    // reserved_recv_sock is a pre-bound socket from PortAllocator.
    explicit UdpAudioPort(const AudioConfig &cfg, int reserved_recv_sock = -1);
    ~UdpAudioPort() override;

    // Bind sockets and start RX/TX worker threads.
    void start();
    // Stop worker threads and close sockets.
    void stop();
    // Snapshot runtime media bridge counters for observability.
    BridgeStats stats_snapshot() const;

    // SIP -> Python (8k -> 16k).
    void onFrameReceived(pj::MediaFrame &frame) override;
    // Python -> SIP (16k -> 8k).
    void onFrameRequested(pj::MediaFrame &frame) override;

  private:
    // Sends buffered 16k audio to Python in 20ms packets.
    void rxSenderLoop();
    // Receives 16k audio from Python and buffers it for SIP playback.
    void txReceiverLoop();

    // Per-call audio configuration.
    AudioConfig cfg_;
    // Reserved socket to prevent port reuse during allocation.
    int reserved_recv_sock_;

    // Ring buffers for jitter smoothing (rx: SIP->Python, tx: Python->SIP).
    class ByteRingBuffer;
    std::unique_ptr<ByteRingBuffer> rx_ring_;
    std::unique_ptr<ByteRingBuffer> tx_ring_;

    // UDP sockets for send/receive.
    int sock_send_;
    int sock_recv_;
    // Cached destination for sendto().
    sockaddr_in send_addr_{};
    // Worker thread state.
    std::atomic<bool> running_{false};
    std::thread rx_thread_;
    std::thread tx_thread_;

    // Ring capacity used for occupancy percentages.
    size_t ring_capacity_bytes_{0};

    // Media bridge telemetry counters.
    std::atomic<uint64_t> rx_udp_packets_sent_{0};
    std::atomic<uint64_t> rx_udp_bytes_sent_{0};
    std::atomic<uint64_t> rx_udp_send_errors_{0};
    std::atomic<uint64_t> rx_ring_bytes_written_{0};
    std::atomic<uint64_t> rx_ring_bytes_dropped_{0};
    std::atomic<uint64_t> rx_ring_high_watermark_bytes_{0};

    std::atomic<uint64_t> tx_udp_packets_received_{0};
    std::atomic<uint64_t> tx_udp_bytes_received_{0};
    std::atomic<uint64_t> tx_udp_recv_errors_{0};
    std::atomic<uint64_t> tx_ring_bytes_written_{0};
    std::atomic<uint64_t> tx_ring_bytes_dropped_{0};
    std::atomic<uint64_t> tx_ring_high_watermark_bytes_{0};

    std::atomic<uint64_t> tx_underrun_frames_{0};
    std::atomic<uint64_t> tx_underrun_bytes_{0};
};
