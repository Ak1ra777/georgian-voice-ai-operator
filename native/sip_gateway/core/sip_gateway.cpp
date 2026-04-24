// Multi-call SIP gateway:
// - Uses PJSUA2 for SIP/RTP.
// - Bridges call audio to Python via per-call raw PCM UDP ports.
// - Uses a two-phase handshake (180 Ringing -> call_ready -> 200 OK).
#include <pjsua2.hpp>
#include <pjmedia/format.h>

#include "core/call_session.hpp"
#include "control/control_channel.hpp"
#include "util/port_allocator.hpp"
#include "core/task_queue.hpp"
#include "media/udp_audio_port.hpp"

#include <atomic>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

using namespace pj;

// Global run flag for clean shutdown (SIGINT/SIGTERM).
std::atomic<bool> g_running{true};

// Read a string env var with a default fallback.
std::string GetEnvOrDefault(const char *name, const std::string &def) {
    const char *value = std::getenv(name);
    return value ? std::string(value) : def;
}

// Lower-case a string for case-insensitive env parsing.
std::string ToLowerCopy(const std::string &input) {
    std::string out = input;
    std::transform(
        out.begin(),
        out.end(),
        out.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return out;
}

// Read an int env var with a default fallback.
int GetEnvIntOrDefault(const char *name, int def) {
    const char *value = std::getenv(name);
    if (!value || !*value) {
        return def;
    }
    try {
        return std::stoi(value);
    } catch (...) {
        return def;
    }
}

// Read a bool env var with a default fallback.
bool GetEnvBoolOrDefault(const char *name, bool def) {
    const char *value = std::getenv(name);
    if (!value || !*value) {
        return def;
    }
    const std::string lower = ToLowerCopy(value);
    if (lower == "1" || lower == "true" || lower == "yes" || lower == "on") {
        return true;
    }
    if (lower == "0" || lower == "false" || lower == "no" || lower == "off") {
        return false;
    }
    return def;
}

std::string ParseTransport(const std::string &transport_raw) {
    const std::string transport = ToLowerCopy(transport_raw);
    if (transport == "udp" || transport == "tcp" || transport == "tls") {
        return transport;
    }
    throw std::runtime_error("Invalid SIP_TRANSPORT: " + transport_raw);
}

std::string ParseSecurityProfile(const std::string &profile_raw) {
    const std::string profile = ToLowerCopy(profile_raw);
    if (profile == "none" || profile == "tls_srtp") {
        return profile;
    }
    throw std::runtime_error("Invalid SIP_SECURITY_PROFILE: " + profile_raw);
}

pjmedia_srtp_use ParseSrtpMode(const std::string &srtp_raw) {
    const std::string srtp_mode = ToLowerCopy(srtp_raw);
    if (srtp_mode == "disabled") {
        return PJMEDIA_SRTP_DISABLED;
    }
    if (srtp_mode == "optional") {
        return PJMEDIA_SRTP_OPTIONAL;
    }
    if (srtp_mode == "mandatory") {
        return PJMEDIA_SRTP_MANDATORY;
    }
    throw std::runtime_error("Invalid SIP_SRTP_MODE: " + srtp_raw);
}

int ParseSrtpSecureSignaling(const std::string &raw) {
    const std::string mode = ToLowerCopy(raw);
    if (mode == "0" || mode == "none") {
        return 0;
    }
    if (mode == "1" || mode == "tls") {
        return 1;
    }
    if (mode == "2" || mode == "sips") {
        return 2;
    }
    throw std::runtime_error("Invalid SIP_SRTP_SECURE_SIGNALING: " + raw);
}

std::string SrtpModeName(pjmedia_srtp_use mode) {
    if (mode == PJMEDIA_SRTP_DISABLED) {
        return "disabled";
    }
    if (mode == PJMEDIA_SRTP_OPTIONAL) {
        return "optional";
    }
    if (mode == PJMEDIA_SRTP_MANDATORY) {
        return "mandatory";
    }
    return "unknown";
}

std::string BoolName(bool value) {
    return value ? "true" : "false";
}

// Signal handler: request shutdown without heavy work.
void HandleSignal(int) {
    g_running.store(false);
}

// Current wall-clock time for logging/telemetry.
int64_t NowEpochMs() {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
    return ms.count();
}

// Generate a stable, canonical session_id for a call (hex string).
std::string NewSessionId() {
    static thread_local std::mt19937 rng{std::random_device{}()};
    std::uniform_int_distribution<int> dist(0, 15);
    std::ostringstream oss;
    for (int i = 0; i < 32; ++i) {
        oss << std::hex << dist(rng);
    }
    return oss.str();
}

// Minimal JSON string escaping for event payloads.
std::string JsonEscape(const std::string &s) {
    std::string out;
    out.reserve(s.size());
    for (char c : s) {
        if (c == '"' || c == '\\') {
            out.push_back('\\');
        }
        out.push_back(c);
    }
    return out;
}

// Simple stdout logging. Keep this lightweight in real-time code.
void Log(const std::string &msg) {
    std::cout << msg << std::endl;
}

// Runtime config collected from environment variables.
struct SipConfig {
    // SIP account credentials and transport.
    std::string domain;
    std::string user;
    std::string password;
    std::string transport;
    std::string security_profile;
    int local_port;
    // Account media security policy.
    pjmedia_srtp_use srtp_mode;
    int srtp_secure_signaling;
    // TLS transport settings.
    std::string tls_ca_file;
    std::string tls_cert_file;
    std::string tls_key_file;
    std::string tls_key_password;
    bool tls_verify_server;
    bool tls_verify_client;
    bool tls_require_client_cert;
    int tls_negotiation_timeout_ms;
    bool tls_enable_renegotiation;
    // Audio bridge defaults (host + ring buffer sizes).
    AudioConfig audio;
    // Unix socket path for the Python control channel.
    std::string control_socket;
    // Backpressure: maximum concurrent calls.
    int max_calls;
    // Timeout for Python readiness before answering.
    int call_ready_timeout_ms;
    // Forced cleanup watchdog after hangup request if disconnect callback is missed.
    int end_cleanup_grace_ms;
    // Reserved UDP port range for per-call bridges.
    int port_base;
    int port_count;
};

class SipApp;

// SIP call object; callbacks are forwarded to SipApp via task queue.
class SipCall : public Call {
  public:
    SipCall(Account &acc, int call_id, SipApp &app)
        : Call(acc, call_id), app_(app), call_id_(call_id) {}

    void onCallState(OnCallStateParam &prm) override;
    void onCallMediaState(OnCallMediaStateParam &prm) override;
    void onDtmfDigit(OnDtmfDigitParam &prm) override;

    int call_id() const { return call_id_; }

  private:
    SipApp &app_;
    int call_id_;
};

// SIP account object; receives incoming call callbacks.
class SipAccount : public Account {
  public:
    explicit SipAccount(SipApp &app) : app_(app) {}

    void onIncomingCall(OnIncomingCallParam &iprm) override;
    void onRegState(OnRegStateParam &prm) override;

  private:
    SipApp &app_;
};

// Main SIP app. Owns per-call sessions, control channel, and task queue.
class SipApp {
  public:
    // Construct app with config, allocator, and control channel.
    explicit SipApp(const SipConfig &cfg)
        : cfg_(cfg),
          port_allocator_(cfg.port_base, cfg.port_count),
          control_(cfg.control_socket,
                   [this](const std::string &session_id) {
                       tasks_.post([this, session_id]() {
                           handleCallReady(session_id);
                       });
                   },
                   [this](const std::string &session_id,
                          const std::string &reason) {
                       tasks_.post([this, session_id, reason]() {
                           handleCallHangup(session_id, reason);
                       });
                   }) {}

    // Initialize PJSUA2, register account, and connect the control channel.
    // This should be called once at process startup.
    void start() {
        EpConfig ep_cfg;
        ep_cfg.logConfig.level = 4;
        ep_cfg.logConfig.consoleLevel = 4;
        ep_.libCreate();
        ep_.libInit(ep_cfg);

        TransportConfig tcfg;
        tcfg.port = cfg_.local_port;
        if (cfg_.transport == "udp") {
            transport_id_ = ep_.transportCreate(PJSIP_TRANSPORT_UDP, tcfg);
        } else if (cfg_.transport == "tcp") {
            transport_id_ = ep_.transportCreate(PJSIP_TRANSPORT_TCP, tcfg);
        } else if (cfg_.transport == "tls") {
            tcfg.tlsConfig.CaListFile = cfg_.tls_ca_file;
            tcfg.tlsConfig.certFile = cfg_.tls_cert_file;
            tcfg.tlsConfig.privKeyFile = cfg_.tls_key_file;
            tcfg.tlsConfig.password = cfg_.tls_key_password;
            tcfg.tlsConfig.verifyServer = cfg_.tls_verify_server;
            tcfg.tlsConfig.verifyClient = cfg_.tls_verify_client;
            tcfg.tlsConfig.requireClientCert = cfg_.tls_require_client_cert;
            tcfg.tlsConfig.msecTimeout =
                static_cast<unsigned>(cfg_.tls_negotiation_timeout_ms);
            tcfg.tlsConfig.enableRenegotiation = cfg_.tls_enable_renegotiation;
            transport_id_ = ep_.transportCreate(PJSIP_TRANSPORT_TLS, tcfg);
        } else {
            throw std::runtime_error("Unsupported SIP_TRANSPORT: " + cfg_.transport);
        }

        Log("sip_security_config"
            " transport=" + cfg_.transport +
            " security_profile=" + cfg_.security_profile +
            " srtp_mode=" + SrtpModeName(cfg_.srtp_mode) +
            " srtp_secure_signaling=" + std::to_string(cfg_.srtp_secure_signaling) +
            " tls_verify_server=" + BoolName(cfg_.tls_verify_server) +
            " tls_verify_client=" + BoolName(cfg_.tls_verify_client) +
            " tls_require_client_cert=" + BoolName(cfg_.tls_require_client_cert) +
            " tls_ca_configured=" + BoolName(!cfg_.tls_ca_file.empty()) +
            " tls_cert_configured=" + BoolName(!cfg_.tls_cert_file.empty()));

        ep_.audDevManager().setNullDev();
        ep_.libStart();

        AccountConfig acfg;
        const bool require_sips = (cfg_.srtp_secure_signaling >= 2);
        const std::string scheme = require_sips ? "sips" : "sip";
        acfg.idUri = scheme + ":" + cfg_.user + "@" + cfg_.domain;
        acfg.regConfig.registrarUri = scheme + ":" + cfg_.domain;
        if (!require_sips && (cfg_.transport == "tcp" || cfg_.transport == "tls")) {
            acfg.regConfig.registrarUri += ";transport=" + cfg_.transport;
        }
        acfg.sipConfig.authCreds.push_back(
            AuthCredInfo("digest", "*", cfg_.user, 0, cfg_.password));
        acfg.sipConfig.transportId = transport_id_;
        acfg.mediaConfig.srtpUse = cfg_.srtp_mode;
        acfg.mediaConfig.srtpSecureSignaling = cfg_.srtp_secure_signaling;

        account_ = std::make_unique<SipAccount>(*this);
        account_->create(acfg);

        // Connect to Python control plane.
        control_.start();
    }

    // Stop all calls and release resources.
    // Safe to call once during shutdown.
    void stop() {
        for (auto &kv : calls_) {
            if (kv.second && kv.second->port) {
                kv.second->port->stop();
            }
        }
        calls_.clear();
        session_index_.clear();
        control_.stop();
        ep_.libDestroy();
    }

    // Poll PJSUA2 events and process queued tasks/timeouts.
    // This drives the entire app loop; call repeatedly.
    void pollEvents() {
        try {
            ep_.libHandleEvents(20);
        } catch (const Error &err) {
            Log(std::string("event_poll_failed ") + err.info());
        }
        tasks_.drain([this](std::exception_ptr eptr) {
            logTaskFailure(eptr);
        });
        tick();
    }

    // Callbacks enqueue work to avoid blocking PJSUA2 threads.
    // These are invoked from SipCall/SipAccount callbacks.
    void enqueueIncomingCall(int call_id) {
        if (!account_) {
            return;
        }
        if (pending_calls_.find(call_id) == pending_calls_.end()) {
            pending_calls_[call_id] =
                std::make_unique<SipCall>(*account_, call_id, *this);
        }
        tasks_.post([this, call_id]() { handleIncomingCallTask(call_id); });
    }

    // Media/state callbacks are forwarded here.
    void enqueueCallMediaState(int call_id) {
        tasks_.post([this, call_id]() { handleCallMediaStateTask(call_id); });
    }

    // Call state callbacks are forwarded here.
    void enqueueCallState(int call_id, bool disconnected) {
        tasks_.post([this, call_id, disconnected]() {
            handleCallStateTask(call_id, disconnected);
        });
    }

    void enqueueCallDtmf(int call_id, std::string digit) {
        tasks_.post([this, call_id, digit = std::move(digit)]() {
            handleCallDtmfTask(call_id, digit);
        });
    }

  private:
    static constexpr std::chrono::seconds kMediaStatsInterval{30};

    void logTaskFailure(std::exception_ptr eptr) {
        if (!eptr) {
            return;
        }
        try {
            std::rethrow_exception(eptr);
        } catch (const Error &err) {
            Log(std::string("task_failed_pjsua2 ") + err.info());
        } catch (const std::exception &ex) {
            Log(std::string("task_failed_std ") + ex.what());
        } catch (...) {
            Log("task_failed_unknown");
        }
    }

    bool tryGetCallInfo(CallSession &session,
                        CallInfo &info,
                        const char *context) {
        try {
            info = session.call->getInfo();
            return true;
        } catch (const Error &err) {
            Log(std::string("call_info_failed ") + context + " " + err.info());
            return false;
        }
    }

    bool safeAnswer(Call &call, const CallOpParam &param, const char *context) {
        try {
            call.answer(param);
            return true;
        } catch (const Error &err) {
            Log(std::string("call_answer_failed ") + context + " " + err.info());
            return false;
        }
    }

    void safeHangup(Call &call, const CallOpParam &param, const char *context) {
        try {
            call.hangup(param);
        } catch (const Error &err) {
            Log(std::string("call_hangup_failed ") + context + " " + err.info());
        }
    }

    void sendGatewayTrace(const CallSession &session,
                          const std::string &event,
                          const std::string &level = "info",
                          const std::string &data_json = "") {
        std::ostringstream oss;
        oss << "{\"type\":\"gateway_trace\""
            << ",\"session_id\":\"" << session.session_id << "\""
            << ",\"event\":\"" << JsonEscape(event) << "\""
            << ",\"level\":\"" << JsonEscape(level) << "\""
            << ",\"call_id\":" << session.call_id
            << ",\"ts_ms\":" << NowEpochMs();
        if (!data_json.empty()) {
            oss << ",\"data\":" << data_json;
        }
        oss << "}";
        control_.send_event(oss.str());
    }

    void logMediaBridgeStats(const CallSession &session,
                             const UdpAudioPort::BridgeStats &stats,
                             const char *event) {
        const uint64_t capacity = stats.ring_capacity_bytes > 0
            ? stats.ring_capacity_bytes
            : 1;
        const uint64_t rx_hwm_pct =
            (stats.rx_ring_high_watermark_bytes * 100) / capacity;
        const uint64_t tx_hwm_pct =
            (stats.tx_ring_high_watermark_bytes * 100) / capacity;
        std::ostringstream data;
        data << "{"
             << "\"sample\":\"" << JsonEscape(event) << "\""
             << ",\"ring_capacity_bytes\":" << stats.ring_capacity_bytes
             << ",\"rx_ring_hwm_bytes\":" << stats.rx_ring_high_watermark_bytes
             << ",\"rx_ring_hwm_pct\":" << rx_hwm_pct
             << ",\"rx_ring_drop_bytes\":" << stats.rx_ring_bytes_dropped
             << ",\"rx_udp_packets_sent\":" << stats.rx_udp_packets_sent
             << ",\"rx_udp_send_errors\":" << stats.rx_udp_send_errors
             << ",\"tx_ring_hwm_bytes\":" << stats.tx_ring_high_watermark_bytes
             << ",\"tx_ring_hwm_pct\":" << tx_hwm_pct
             << ",\"tx_ring_drop_bytes\":" << stats.tx_ring_bytes_dropped
             << ",\"tx_udp_packets_received\":" << stats.tx_udp_packets_received
             << ",\"tx_udp_recv_errors\":" << stats.tx_udp_recv_errors
             << ",\"tx_underrun_frames\":" << stats.tx_underrun_frames
             << ",\"tx_underrun_bytes\":" << stats.tx_underrun_bytes
             << "}";
        sendGatewayTrace(session, "call_media_bridge_stats", "info", data.str());
        Log("call_media_bridge_stats event=" + std::string(event) +
            " session_id=" + session.session_id +
            " call_id=" + std::to_string(session.call_id) +
            " ring_capacity_bytes=" + std::to_string(stats.ring_capacity_bytes) +
            " rx_ring_hwm_bytes=" + std::to_string(stats.rx_ring_high_watermark_bytes) +
            " rx_ring_hwm_pct=" + std::to_string(rx_hwm_pct) +
            " rx_ring_drop_bytes=" + std::to_string(stats.rx_ring_bytes_dropped) +
            " rx_udp_packets_sent=" + std::to_string(stats.rx_udp_packets_sent) +
            " rx_udp_send_errors=" + std::to_string(stats.rx_udp_send_errors) +
            " tx_ring_hwm_bytes=" + std::to_string(stats.tx_ring_high_watermark_bytes) +
            " tx_ring_hwm_pct=" + std::to_string(tx_hwm_pct) +
            " tx_ring_drop_bytes=" + std::to_string(stats.tx_ring_bytes_dropped) +
            " tx_udp_packets_received=" + std::to_string(stats.tx_udp_packets_received) +
            " tx_udp_recv_errors=" + std::to_string(stats.tx_udp_recv_errors) +
            " tx_underrun_frames=" + std::to_string(stats.tx_underrun_frames) +
            " tx_underrun_bytes=" + std::to_string(stats.tx_underrun_bytes));
    }

    std::unique_ptr<SipCall> takePendingCall(int call_id) {
        auto it = pending_calls_.find(call_id);
        if (it == pending_calls_.end()) {
            return nullptr;
        }
        auto call = std::move(it->second);
        pending_calls_.erase(it);
        return call;
    }

    // Allocate session, send 180 Ringing, and notify Python of ports.
    void handleIncomingCallTask(int call_id) {
        auto call = takePendingCall(call_id);
        if (!call) {
            // If we somehow missed registration, try to attach anyway.
            call = std::make_unique<SipCall>(*account_, call_id, *this);
        }

        // Backpressure: reject if max concurrent calls reached.
        if (static_cast<int>(calls_.size()) >= cfg_.max_calls) {
            CallOpParam param;
            param.statusCode = PJSIP_SC_BUSY_HERE;
            safeHangup(*call, param, "busy");
            Log("call_rejected_busy call_id=" + std::to_string(call_id));
            return;
        }

        // Allocate a per-call UDP port pair (bind-checked reservation).
        auto reservation = port_allocator_.reserve_pair();
        if (!reservation) {
            CallOpParam param;
            param.statusCode = PJSIP_SC_TEMPORARILY_UNAVAILABLE;
            safeHangup(*call, param, "no_ports");
            Log("call_rejected_no_ports call_id=" + std::to_string(call_id));
            return;
        }

        // Create per-call session state with canonical session_id.
        auto session = std::make_unique<CallSession>();
        session->session_id = NewSessionId();
        session->call_id = call_id;
        session->reservation = std::move(reservation);
        session->audio_cfg = cfg_.audio;
        session->audio_cfg.rx_port = session->reservation->rx_port;
        session->audio_cfg.tx_port = session->reservation->tx_port;
        session->created_at = std::chrono::steady_clock::now();
        session->ready_deadline = session->created_at +
            std::chrono::milliseconds(cfg_.call_ready_timeout_ms);
        session->allocated = true;

        // Create call object and send provisional response (180 Ringing).
        session->call = std::move(call);
        CallOpParam ringing;
        ringing.statusCode = PJSIP_SC_RINGING;
        if (!safeAnswer(*session->call, ringing, "ringing")) {
            port_allocator_.release_pair(*session->reservation);
            return;
        }

        // Capture remote identity for observability/debugging.
        CallInfo info;
        if (tryGetCallInfo(*session, info, "incoming")) {
            session->remote_uri = info.remoteUri;
        }

        // Index by session_id for control-channel lookup.
        session_index_[session->session_id] = call_id;
        calls_[call_id] = std::move(session);

        // Notify Python of the allocation and the per-call port pair.
        auto &stored = *calls_[call_id];
        std::ostringstream oss;
        oss << "{\"type\":\"call_allocated\""
            << ",\"session_id\":\"" << stored.session_id << "\""
            << ",\"call_id\":" << stored.call_id
            << ",\"rx_port\":" << stored.audio_cfg.rx_port
            << ",\"tx_port\":" << stored.audio_cfg.tx_port
            << ",\"remote_uri\":\"" << JsonEscape(stored.remote_uri) << "\""
            << ",\"start_ts_ms\":" << NowEpochMs()
            << "}";
        control_.send_event(oss.str());
        Log("call_allocated session_id=" + stored.session_id +
            " call_id=" + std::to_string(stored.call_id) +
            " rx_port=" + std::to_string(stored.audio_cfg.rx_port) +
            " tx_port=" + std::to_string(stored.audio_cfg.tx_port));
    }

    // Media is active; start bridge if Python is ready.
    void handleCallMediaStateTask(int call_id) {
        auto it = calls_.find(call_id);
        if (it == calls_.end()) {
            return;
        }
        auto &session = *it->second;
        if (session.ended) {
            return;
        }

        // Find the active audio media index for this call.
        CallInfo info;
        if (!tryGetCallInfo(session, info, "media_state")) {
            endSession(call_id, "call_info_failed");
            return;
        }
        for (unsigned i = 0; i < info.media.size(); ++i) {
            if (info.media[i].type != PJMEDIA_TYPE_AUDIO ||
                info.media[i].status != PJSUA_CALL_MEDIA_ACTIVE) {
                continue;
            }
            session.media_active = true;
            session.media_index = static_cast<int>(i);
            break;
        }

        // Try to start the bridge now that media is active.
        tryStartMediaBridge(session);
    }

    // Call state changed; cleanup on disconnect.
    void handleCallStateTask(int call_id, bool disconnected) {
        auto it = calls_.find(call_id);
        if (it == calls_.end()) {
            return;
        }
        auto &session = *it->second;
        if (session.ended) {
            return;
        }
        if (!disconnected) {
            return;
        }
        std::string reason = session.end_reason.empty()
            ? "disconnected"
            : session.end_reason;
        endSession(call_id, reason);
    }

    void handleCallDtmfTask(int call_id, const std::string &digit) {
        if (digit.empty()) {
            return;
        }
        auto it = calls_.find(call_id);
        if (it == calls_.end()) {
            return;
        }
        auto &session = *it->second;
        if (session.ended || session.end_requested) {
            return;
        }

        std::ostringstream oss;
        oss << "{\"type\":\"dtmf_key\""
            << ",\"session_id\":\"" << session.session_id << "\""
            << ",\"digit\":\"" << JsonEscape(digit) << "\""
            << ",\"call_id\":" << session.call_id
            << ",\"ts_ms\":" << NowEpochMs()
            << "}";
        control_.send_event(oss.str());
        Log("call_dtmf session_id=" + session.session_id +
            " call_id=" + std::to_string(session.call_id) +
            " digit=" + digit);
    }

    // Python reported readiness; answer and start media if possible.
    void handleCallReady(const std::string &session_id) {
        auto it = session_index_.find(session_id);
        if (it == session_index_.end()) {
            return;
        }
        auto sit = calls_.find(it->second);
        if (sit == calls_.end()) {
            return;
        }
        auto &session = *sit->second;
        if (session.ended || session.end_requested) {
            return;
        }
        // Mark Python ready and answer the call.
        session.py_ready = true;
        sendGatewayTrace(session, "call_ready");
        Log("call_ready session_id=" + session.session_id +
            " call_id=" + std::to_string(session.call_id));
        answerCall(session);
        tryStartMediaBridge(session);
    }

    // Python requested hangup for this session (timeout, policy, etc.).
    void handleCallHangup(const std::string &session_id,
                          const std::string &reason) {
        auto it = session_index_.find(session_id);
        if (it == session_index_.end()) {
            return;
        }
        auto sit = calls_.find(it->second);
        if (sit == calls_.end()) {
            return;
        }
        auto &session = *sit->second;
        if (session.ended || session.end_requested) {
            return;
        }

        std::string end_reason = "python_hangup";
        if (!reason.empty()) {
            end_reason += ":" + reason;
        }
        std::ostringstream data;
        data << "{\"reason\":\"" << JsonEscape(end_reason) << "\"}";
        sendGatewayTrace(session, "call_hangup_requested", "info", data.str());
        Log("call_hangup_requested session_id=" + session.session_id +
            " call_id=" + std::to_string(session.call_id) +
            " reason=" + end_reason);
        requestEnd(session, end_reason, PJSIP_SC_OK);
    }

    // Final answer (200 OK). Called only after Python is ready.
    void answerCall(CallSession &session) {
        if (session.answered || session.ended) {
            return;
        }
        // Send final 200 OK to establish the SIP dialog.
        CallOpParam ok;
        ok.statusCode = PJSIP_SC_OK;
        if (!safeAnswer(*session.call, ok, "ok")) {
            return;
        }
        session.answered = true;
    }

    // Start the UDP bridge once both py_ready and media_active are true.
    // This function is idempotent and safe to call multiple times.
    void tryStartMediaBridge(CallSession &session) {
        if (session.media_started || session.ended || session.end_requested) {
            return;
        }
        if (!session.py_ready || !session.media_active) {
            return;
        }
        if (!session.reservation) {
            requestEnd(session, "port_reservation_missing", PJSIP_SC_TEMPORARILY_UNAVAILABLE);
            return;
        }
        try {
            // Convert reservation into a live UDP port and start worker threads.
            int recv_sock = session.reservation->take_recv_sock();
            session.port = std::make_unique<UdpAudioPort>(session.audio_cfg, recv_sock);
            session.port->start();

            if (session.media_index < 0) {
                requestEnd(session, "media_index_missing", PJSIP_SC_TEMPORARILY_UNAVAILABLE);
                return;
            }
            // Attach the audio port to the PJSUA2 media stream (bidirectional).
            auto *media = session.call->getMedia(session.media_index);
            auto *aud_med = dynamic_cast<AudioMedia *>(media);
            if (!aud_med) {
                requestEnd(session, "audio_media_missing", PJSIP_SC_TEMPORARILY_UNAVAILABLE);
                return;
            }
            aud_med->startTransmit(*session.port);
            session.port->startTransmit(*aud_med);
            session.media_started = true;
            session.media_stats_deadline =
                std::chrono::steady_clock::now() + kMediaStatsInterval;

            // Notify Python that media is flowing.
            std::ostringstream oss;
            oss << "{\"type\":\"call_media_started\""
                << ",\"session_id\":\"" << session.session_id << "\""
                << ",\"call_id\":" << session.call_id
                << ",\"ts_ms\":" << NowEpochMs()
                << "}";
            control_.send_event(oss.str());
            Log("call_media_started session_id=" + session.session_id +
                " call_id=" + std::to_string(session.call_id));
        } catch (const Error &err) {
            Log(std::string("media_start_failed_pjsua2 ") + err.info());
            requestEnd(session, "media_start_failed", PJSIP_SC_TEMPORARILY_UNAVAILABLE);
        } catch (const std::exception &ex) {
            Log(std::string("media_start_failed ") + ex.what());
            requestEnd(session, "media_start_failed", PJSIP_SC_TEMPORARILY_UNAVAILABLE);
        } catch (...) {
            Log("media_start_failed_unknown");
            requestEnd(session, "media_start_failed", PJSIP_SC_TEMPORARILY_UNAVAILABLE);
        }
    }

    // Request call end; hangup is sent once, then handled in state callback.
    void requestEnd(CallSession &session, const std::string &reason, pjsip_status_code code) {
        if (session.end_requested || session.ended) {
            return;
        }
        session.end_requested = true;
        session.end_reason = reason;
        session.end_request_deadline = std::chrono::steady_clock::now() +
            std::chrono::milliseconds(cfg_.end_cleanup_grace_ms);
        std::ostringstream data;
        data << "{"
             << "\"reason\":\"" << JsonEscape(reason) << "\""
             << ",\"cleanup_grace_ms\":" << cfg_.end_cleanup_grace_ms
             << "}";
        sendGatewayTrace(session, "call_end_requested", "info", data.str());
        Log("call_end_requested session_id=" + session.session_id +
            " call_id=" + std::to_string(session.call_id) +
            " reason=" + reason +
            " cleanup_grace_ms=" + std::to_string(cfg_.end_cleanup_grace_ms));

        // Send SIP hangup; actual cleanup happens in handleCallStateTask.
        CallOpParam param;
        param.statusCode = code;
        safeHangup(*session.call, param, "request_end");
    }

    // Final cleanup after disconnect or timeout.
    void endSession(int call_id, const std::string &reason) {
        std::unique_ptr<CallSession> session;
        auto it = calls_.find(call_id);
        if (it == calls_.end()) {
            return;
        }
        session = std::move(it->second);
        calls_.erase(it);
        session_index_.erase(session->session_id);

        session->ended = true;
        session->end_reason = reason;

        // Stop media bridge and release reserved ports.
        if (session->port) {
            session->port->stop();
            logMediaBridgeStats(*session, session->port->stats_snapshot(), "final");
        }
        if (session->reservation) {
            port_allocator_.release_pair(*session->reservation);
        }

        std::ostringstream oss;
        oss << "{\"type\":\"call_end\""
            << ",\"session_id\":\"" << session->session_id << "\""
            << ",\"reason\":\"" << JsonEscape(reason) << "\""
            << ",\"call_id\":" << session->call_id
            << ",\"ts_ms\":" << NowEpochMs()
            << "}";
        // Notify Python that the call ended.
        control_.send_event(oss.str());
        Log("call_end session_id=" + session->session_id +
            " call_id=" + std::to_string(session->call_id) +
            " reason=" + reason);
    }

    // Enforce call-ready timeout without blocking callbacks.
    void tick() {
        auto now = std::chrono::steady_clock::now();
        std::vector<int> force_cleanup_call_ids;
        for (auto &kv : calls_) {
            auto &session = *kv.second;
            if (session.ended) {
                continue;
            }
            if (session.media_started &&
                session.media_stats_deadline.time_since_epoch().count() > 0 &&
                now > session.media_stats_deadline &&
                session.port) {
                logMediaBridgeStats(session, session.port->stats_snapshot(), "periodic");
                session.media_stats_deadline = now + kMediaStatsInterval;
            }
            if (session.end_requested) {
                if (session.end_request_deadline.time_since_epoch().count() > 0 &&
                    now > session.end_request_deadline) {
                    force_cleanup_call_ids.push_back(session.call_id);
                }
                continue;
            }
            // If Python isn't ready in time, end the call gracefully.
            if (!session.py_ready && now > session.ready_deadline) {
                requestEnd(session, "call_ready_timeout", PJSIP_SC_TEMPORARILY_UNAVAILABLE);
            }
        }
        for (int call_id : force_cleanup_call_ids) {
            auto it = calls_.find(call_id);
            if (it == calls_.end()) {
                continue;
            }
            auto &session = *it->second;
            if (session.ended || !session.end_requested) {
                continue;
            }
            const std::string reason = session.end_reason.empty()
                ? "forced_cleanup_timeout"
                : session.end_reason + ":forced_cleanup_timeout";
            std::ostringstream data;
            data << "{\"reason\":\"" << JsonEscape(reason) << "\"}";
            sendGatewayTrace(session, "call_force_cleanup", "warning", data.str());
            Log("call_force_cleanup session_id=" + session.session_id +
                " call_id=" + std::to_string(session.call_id) +
                " reason=" + reason);
            endSession(call_id, reason);
        }
    }

    SipConfig cfg_;
    Endpoint ep_;
    std::unique_ptr<SipAccount> account_;
    int transport_id_{-1};

    // All PJSUA2 callbacks post work here; drained in pollEvents().
    TaskQueue tasks_;
    std::unordered_map<int, std::unique_ptr<CallSession>> calls_;
    std::unordered_map<std::string, int> session_index_;
    std::unordered_map<int, std::unique_ptr<SipCall>> pending_calls_;

    PortAllocator port_allocator_;
    ControlChannel control_;
};

void SipCall::onCallState(OnCallStateParam &prm) {
    PJ_UNUSED_ARG(prm);
    bool disconnected = false;
    try {
        CallInfo info = getInfo();
        disconnected = (info.state == PJSIP_INV_STATE_DISCONNECTED);
    } catch (const Error &) {
        // If info isn't available, treat it as disconnected so we can cleanup.
        disconnected = true;
    }
    // Forward to SipApp via task queue to keep callback lightweight.
    app_.enqueueCallState(call_id_, disconnected);
}

void SipCall::onCallMediaState(OnCallMediaStateParam &prm) {
    PJ_UNUSED_ARG(prm);
    // Forward to SipApp via task queue to keep callback lightweight.
    app_.enqueueCallMediaState(call_id_);
}

void SipCall::onDtmfDigit(OnDtmfDigitParam &prm) {
    for (char digit : prm.digit) {
        app_.enqueueCallDtmf(call_id_, std::string(1, digit));
    }
}

void SipAccount::onIncomingCall(OnIncomingCallParam &iprm) {
    // Forward to SipApp via task queue to keep callback lightweight.
    app_.enqueueIncomingCall(iprm.callId);
}

void SipAccount::onRegState(OnRegStateParam &prm) {
    // Registration status reporting.
    std::cout << "Registration: "
              << (prm.code / 100 == 2 ? "OK" : "FAILED") << " ("
              << prm.code << ")" << std::endl;
}

SipConfig LoadSipConfig() {
    // Load all runtime config from environment variables.
    SipConfig cfg;
    cfg.domain = GetEnvOrDefault("SIP_DOMAIN", "");
    cfg.user = GetEnvOrDefault("SIP_USER", "");
    cfg.password = GetEnvOrDefault("SIP_PASS", "");
    cfg.security_profile =
        ParseSecurityProfile(GetEnvOrDefault("SIP_SECURITY_PROFILE", "none"));

    std::string selected_transport =
        ParseTransport(GetEnvOrDefault("SIP_TRANSPORT", "udp"));
    if (cfg.security_profile == "tls_srtp") {
        selected_transport = "tls";
    }
    cfg.transport = selected_transport;
    const int default_port = (cfg.transport == "tls") ? 5061 : 5060;
    cfg.local_port = GetEnvIntOrDefault("SIP_LOCAL_PORT", default_port);
    if (cfg.local_port <= 0 || cfg.local_port > 65535) {
        throw std::runtime_error("SIP_LOCAL_PORT must be within 1..65535");
    }

    const std::string default_srtp_mode =
        (cfg.security_profile == "tls_srtp") ? "mandatory" : "disabled";
    cfg.srtp_mode = ParseSrtpMode(GetEnvOrDefault("SIP_SRTP_MODE", default_srtp_mode));

    const std::string default_secure_signaling =
        (cfg.security_profile == "tls_srtp") ? "tls" : "none";
    cfg.srtp_secure_signaling =
        ParseSrtpSecureSignaling(GetEnvOrDefault(
            "SIP_SRTP_SECURE_SIGNALING", default_secure_signaling));

    cfg.tls_ca_file = GetEnvOrDefault("SIP_TLS_CA_FILE", "");
    cfg.tls_cert_file = GetEnvOrDefault("SIP_TLS_CERT_FILE", "");
    cfg.tls_key_file = GetEnvOrDefault("SIP_TLS_KEY_FILE", "");
    cfg.tls_key_password = GetEnvOrDefault("SIP_TLS_KEY_PASSWORD", "");
    cfg.tls_verify_server = GetEnvBoolOrDefault(
        "SIP_TLS_VERIFY_SERVER", cfg.security_profile == "tls_srtp");
    cfg.tls_verify_client = GetEnvBoolOrDefault("SIP_TLS_VERIFY_CLIENT", false);
    cfg.tls_require_client_cert =
        GetEnvBoolOrDefault("SIP_TLS_REQUIRE_CLIENT_CERT", false);
    cfg.tls_negotiation_timeout_ms = std::max(
        0, GetEnvIntOrDefault("SIP_TLS_NEGOTIATION_TIMEOUT_MS", 5000));
    cfg.tls_enable_renegotiation =
        GetEnvBoolOrDefault("SIP_TLS_ENABLE_RENEGOTIATION", false);

    if (cfg.security_profile == "tls_srtp") {
        // Secure profile is intentionally strict.
        cfg.transport = "tls";
        cfg.srtp_mode = PJMEDIA_SRTP_MANDATORY;
        if (cfg.srtp_secure_signaling < 1) {
            cfg.srtp_secure_signaling = 1;
        }
        cfg.tls_verify_server = true;
    }

    if (cfg.srtp_mode == PJMEDIA_SRTP_DISABLED && cfg.srtp_secure_signaling != 0) {
        throw std::runtime_error(
            "SIP_SRTP_SECURE_SIGNALING must be 'none' when SIP_SRTP_MODE=disabled");
    }
    if (cfg.srtp_secure_signaling > 0 && cfg.transport != "tls") {
        throw std::runtime_error(
            "SIP_SRTP_SECURE_SIGNALING requires SIP_TRANSPORT=tls");
    }
    if (cfg.transport != "tls" && cfg.tls_verify_server) {
        // Preserve intent while avoiding surprising validation on non-TLS modes.
        cfg.tls_verify_server = false;
    }

    cfg.audio.host = GetEnvOrDefault("SIP_AUDIO_HOST", "127.0.0.1");
    cfg.audio.rx_port = 0;
    cfg.audio.tx_port = 0;
    cfg.audio.ring_packets = static_cast<size_t>(
        GetEnvIntOrDefault("SIP_AUDIO_RING_PACKETS", 50));

    cfg.control_socket = GetEnvOrDefault("SIP_CONTROL_SOCKET", "/tmp/demo_voice_assistant.sock");
    cfg.max_calls = GetEnvIntOrDefault("MAX_CALLS", 20);
    cfg.call_ready_timeout_ms = GetEnvIntOrDefault("CALL_READY_TIMEOUT_MS", 2000);
    cfg.end_cleanup_grace_ms = GetEnvIntOrDefault("CALL_END_CLEANUP_GRACE_MS", 5000);
    if (cfg.end_cleanup_grace_ms < 100) {
        cfg.end_cleanup_grace_ms = 100;
    }
    cfg.port_base = GetEnvIntOrDefault("SIP_AUDIO_PORT_BASE", 41000);
    cfg.port_count = GetEnvIntOrDefault("SIP_AUDIO_PORT_COUNT", 2000);
    // Ensure a valid even-sized range (pairs of ports).
    if (cfg.port_count < 2) {
        cfg.port_count = 2;
    }
    if (cfg.port_count % 2 != 0) {
        cfg.port_count -= 1;
    }

    return cfg;
}

int main() {
    // Basic lifecycle: init -> run event loop -> shutdown.
    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    try {
        SipConfig cfg = LoadSipConfig();
        if (cfg.domain.empty() || cfg.user.empty() || cfg.password.empty()) {
            throw std::runtime_error("Missing SIP_DOMAIN, SIP_USER, or SIP_PASS");
        }

        SipApp app(cfg);
        app.start();

        while (g_running.load()) {
            app.pollEvents();
        }

        app.stop();
    } catch (const Error &err) {
        std::cerr << "PJSUA2 error: " << err.info() << std::endl;
        return 1;
    } catch (const std::exception &ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
