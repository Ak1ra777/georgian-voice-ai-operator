#pragma once

// Per-call state container for the SIP gateway.
// Holds canonical session_id, audio bridge config, and call lifecycle flags.

#include "util/port_allocator.hpp"
#include "media/udp_audio_port.hpp"

#include <chrono>
#include <memory>
#include <string>

class SipCall;

struct CallSession {
    // Canonical identifier used across the whole system.
    std::string session_id;
    // PJSUA2 call_id (routing only; not stable across replacements).
    int call_id{-1};
    // Remote SIP URI for debugging/observability.
    std::string remote_uri;

    // Per-call UDP audio configuration.
    AudioConfig audio_cfg{};
    // Reserved UDP port pair (kept until bridge starts).
    std::unique_ptr<PortReservation> reservation;
    // Active audio bridge for this call (created once media starts).
    std::unique_ptr<UdpAudioPort> port;
    // PJSUA2 call object for this session.
    std::unique_ptr<SipCall> call;

    // State flags (idempotent transitions).
    bool allocated{false};
    bool py_ready{false};
    bool media_active{false};
    bool media_started{false};
    bool answered{false};
    bool end_requested{false};
    bool ended{false};

    // Active media index in the PJSUA2 call.
    int media_index{-1};
    // Reason for termination (timeout, disconnect, error).
    std::string end_reason;

    // Timing used for timeouts and observability.
    std::chrono::steady_clock::time_point created_at;
    std::chrono::steady_clock::time_point ready_deadline;
    std::chrono::steady_clock::time_point end_request_deadline;
    std::chrono::steady_clock::time_point media_stats_deadline;
};
