#include "control/control_channel.hpp"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

std::string ParseError(const char *message, size_t pos) {
    return std::string(message) + " at_pos=" + std::to_string(pos);
}

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

bool SkipWhitespace(const std::string &line, size_t &pos) {
    while (pos < line.size() &&
           std::isspace(static_cast<unsigned char>(line[pos])) != 0) {
        ++pos;
    }
    return pos <= line.size();
}

int HexValue(char c) {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return 10 + (c - 'a');
    }
    if (c >= 'A' && c <= 'F') {
        return 10 + (c - 'A');
    }
    return -1;
}

bool ParseHex4(const std::string &line, size_t &pos, uint32_t &code_point) {
    if (pos + 4 > line.size()) {
        return false;
    }
    uint32_t value = 0;
    for (int i = 0; i < 4; ++i) {
        int hv = HexValue(line[pos + static_cast<size_t>(i)]);
        if (hv < 0) {
            return false;
        }
        value = (value << 4) | static_cast<uint32_t>(hv);
    }
    pos += 4;
    code_point = value;
    return true;
}

void AppendUtf8(std::string &out, uint32_t code_point) {
    if (code_point <= 0x7F) {
        out.push_back(static_cast<char>(code_point));
        return;
    }
    if (code_point <= 0x7FF) {
        out.push_back(static_cast<char>(0xC0 | ((code_point >> 6) & 0x1F)));
        out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
        return;
    }
    if (code_point <= 0xFFFF) {
        out.push_back(static_cast<char>(0xE0 | ((code_point >> 12) & 0x0F)));
        out.push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
        return;
    }
    if (code_point <= 0x10FFFF) {
        out.push_back(static_cast<char>(0xF0 | ((code_point >> 18) & 0x07)));
        out.push_back(static_cast<char>(0x80 | ((code_point >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((code_point >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (code_point & 0x3F)));
        return;
    }
    // Unicode replacement char.
    out.append("\xEF\xBF\xBD");
}

bool ParseJsonString(const std::string &line,
                     size_t &pos,
                     std::string &out,
                     std::string &error) {
    if (pos >= line.size() || line[pos] != '"') {
        error = ParseError("expected_string", pos);
        return false;
    }
    ++pos;
    out.clear();
    while (pos < line.size()) {
        char c = line[pos++];
        if (c == '"') {
            return true;
        }
        if (c == '\\') {
            if (pos >= line.size()) {
                error = ParseError("unterminated_escape", pos);
                return false;
            }
            char esc = line[pos++];
            switch (esc) {
                case '"':
                case '\\':
                case '/':
                    out.push_back(esc);
                    break;
                case 'b':
                    out.push_back('\b');
                    break;
                case 'f':
                    out.push_back('\f');
                    break;
                case 'n':
                    out.push_back('\n');
                    break;
                case 'r':
                    out.push_back('\r');
                    break;
                case 't':
                    out.push_back('\t');
                    break;
                case 'u': {
                    uint32_t code_point = 0;
                    if (!ParseHex4(line, pos, code_point)) {
                        error = ParseError("invalid_unicode_escape", pos);
                        return false;
                    }
                    if (code_point >= 0xD800 && code_point <= 0xDBFF) {
                        // Handle surrogate pair.
                        if (pos + 6 <= line.size() && line[pos] == '\\' &&
                            line[pos + 1] == 'u') {
                            size_t low_pos = pos + 2;
                            uint32_t low_surrogate = 0;
                            if (ParseHex4(line, low_pos, low_surrogate) &&
                                low_surrogate >= 0xDC00 &&
                                low_surrogate <= 0xDFFF) {
                                code_point =
                                    0x10000 +
                                    (((code_point - 0xD800) << 10) |
                                     (low_surrogate - 0xDC00));
                                pos = low_pos;
                            }
                        }
                    }
                    AppendUtf8(out, code_point);
                    break;
                }
                default:
                    error = ParseError("invalid_escape", pos);
                    return false;
            }
            continue;
        }
        if (static_cast<unsigned char>(c) < 0x20) {
            error = ParseError("control_char_in_string", pos);
            return false;
        }
        out.push_back(c);
    }
    error = ParseError("unterminated_string", pos);
    return false;
}

bool ParseJsonValue(const std::string &line, size_t &pos, std::string &error);

bool ParseJsonLiteral(const std::string &line,
                      size_t &pos,
                      const char *literal) {
    size_t i = 0;
    while (literal[i] != '\0') {
        if (pos >= line.size() || line[pos] != literal[i]) {
            return false;
        }
        ++pos;
        ++i;
    }
    return true;
}

bool ParseJsonNumber(const std::string &line, size_t &pos) {
    size_t i = pos;
    if (i < line.size() && line[i] == '-') {
        ++i;
    }
    if (i >= line.size()) {
        return false;
    }
    if (line[i] == '0') {
        ++i;
    } else if (line[i] >= '1' && line[i] <= '9') {
        while (i < line.size() && std::isdigit(static_cast<unsigned char>(line[i])) != 0) {
            ++i;
        }
    } else {
        return false;
    }

    if (i < line.size() && line[i] == '.') {
        ++i;
        if (i >= line.size() ||
            std::isdigit(static_cast<unsigned char>(line[i])) == 0) {
            return false;
        }
        while (i < line.size() && std::isdigit(static_cast<unsigned char>(line[i])) != 0) {
            ++i;
        }
    }

    if (i < line.size() && (line[i] == 'e' || line[i] == 'E')) {
        ++i;
        if (i < line.size() && (line[i] == '+' || line[i] == '-')) {
            ++i;
        }
        if (i >= line.size() ||
            std::isdigit(static_cast<unsigned char>(line[i])) == 0) {
            return false;
        }
        while (i < line.size() && std::isdigit(static_cast<unsigned char>(line[i])) != 0) {
            ++i;
        }
    }

    pos = i;
    return true;
}

bool ParseJsonArray(const std::string &line, size_t &pos, std::string &error) {
    if (pos >= line.size() || line[pos] != '[') {
        error = ParseError("expected_array", pos);
        return false;
    }
    ++pos;
    SkipWhitespace(line, pos);
    if (pos < line.size() && line[pos] == ']') {
        ++pos;
        return true;
    }
    while (pos < line.size()) {
        if (!ParseJsonValue(line, pos, error)) {
            return false;
        }
        SkipWhitespace(line, pos);
        if (pos >= line.size()) {
            error = ParseError("unterminated_array", pos);
            return false;
        }
        if (line[pos] == ',') {
            ++pos;
            SkipWhitespace(line, pos);
            continue;
        }
        if (line[pos] == ']') {
            ++pos;
            return true;
        }
        error = ParseError("expected_array_delimiter", pos);
        return false;
    }
    error = ParseError("unterminated_array", pos);
    return false;
}

bool ParseJsonObjectSkip(const std::string &line, size_t &pos, std::string &error) {
    if (pos >= line.size() || line[pos] != '{') {
        error = ParseError("expected_object", pos);
        return false;
    }
    ++pos;
    SkipWhitespace(line, pos);
    if (pos < line.size() && line[pos] == '}') {
        ++pos;
        return true;
    }
    while (pos < line.size()) {
        std::string key;
        if (!ParseJsonString(line, pos, key, error)) {
            return false;
        }
        SkipWhitespace(line, pos);
        if (pos >= line.size() || line[pos] != ':') {
            error = ParseError("expected_colon", pos);
            return false;
        }
        ++pos;
        SkipWhitespace(line, pos);
        if (!ParseJsonValue(line, pos, error)) {
            return false;
        }
        SkipWhitespace(line, pos);
        if (pos >= line.size()) {
            error = ParseError("unterminated_object", pos);
            return false;
        }
        if (line[pos] == ',') {
            ++pos;
            SkipWhitespace(line, pos);
            continue;
        }
        if (line[pos] == '}') {
            ++pos;
            return true;
        }
        error = ParseError("expected_object_delimiter", pos);
        return false;
    }
    error = ParseError("unterminated_object", pos);
    return false;
}

bool ParseJsonValue(const std::string &line, size_t &pos, std::string &error) {
    SkipWhitespace(line, pos);
    if (pos >= line.size()) {
        error = ParseError("unexpected_eof", pos);
        return false;
    }
    const char c = line[pos];
    if (c == '"') {
        std::string ignored;
        return ParseJsonString(line, pos, ignored, error);
    }
    if (c == '{') {
        return ParseJsonObjectSkip(line, pos, error);
    }
    if (c == '[') {
        return ParseJsonArray(line, pos, error);
    }
    if (c == 't') {
        if (!ParseJsonLiteral(line, pos, "true")) {
            error = ParseError("invalid_literal", pos);
            return false;
        }
        return true;
    }
    if (c == 'f') {
        if (!ParseJsonLiteral(line, pos, "false")) {
            error = ParseError("invalid_literal", pos);
            return false;
        }
        return true;
    }
    if (c == 'n') {
        if (!ParseJsonLiteral(line, pos, "null")) {
            error = ParseError("invalid_literal", pos);
            return false;
        }
        return true;
    }
    if (!ParseJsonNumber(line, pos)) {
        error = ParseError("invalid_value", pos);
        return false;
    }
    return true;
}

bool ParseJsonObjectStringFields(const std::string &line,
                                 std::unordered_map<std::string, std::string> &fields,
                                 std::string &error) {
    size_t pos = 0;
    fields.clear();
    SkipWhitespace(line, pos);
    if (pos >= line.size() || line[pos] != '{') {
        error = ParseError("expected_object_root", pos);
        return false;
    }
    ++pos;
    SkipWhitespace(line, pos);
    if (pos < line.size() && line[pos] == '}') {
        ++pos;
        SkipWhitespace(line, pos);
        return pos == line.size();
    }

    while (pos < line.size()) {
        std::string key;
        if (!ParseJsonString(line, pos, key, error)) {
            return false;
        }
        SkipWhitespace(line, pos);
        if (pos >= line.size() || line[pos] != ':') {
            error = ParseError("expected_colon", pos);
            return false;
        }
        ++pos;
        SkipWhitespace(line, pos);
        if (pos >= line.size()) {
            error = ParseError("unexpected_eof", pos);
            return false;
        }

        if (line[pos] == '"') {
            std::string value;
            if (!ParseJsonString(line, pos, value, error)) {
                return false;
            }
            fields[key] = value;
        } else {
            if (!ParseJsonValue(line, pos, error)) {
                return false;
            }
        }

        SkipWhitespace(line, pos);
        if (pos >= line.size()) {
            error = ParseError("unterminated_object", pos);
            return false;
        }
        if (line[pos] == ',') {
            ++pos;
            SkipWhitespace(line, pos);
            continue;
        }
        if (line[pos] == '}') {
            ++pos;
            SkipWhitespace(line, pos);
            if (pos != line.size()) {
                error = ParseError("trailing_data", pos);
                return false;
            }
            return true;
        }
        error = ParseError("expected_object_delimiter", pos);
        return false;
    }

    error = ParseError("unterminated_object", pos);
    return false;
}

std::string GetStringField(const std::unordered_map<std::string, std::string> &fields,
                           const std::string &key) {
    auto it = fields.find(key);
    if (it == fields.end()) {
        return "";
    }
    return it->second;
}

}  // namespace

// Construct the control channel client; does not connect yet.
ControlChannel::ControlChannel(std::string socket_path,
                               ReadyCallback on_ready,
                               HangupCallback on_hangup)
    : socket_path_(std::move(socket_path)),
      on_ready_(std::move(on_ready)),
      on_hangup_(std::move(on_hangup)) {}

// Ensure socket and reader thread are stopped on destruction.
ControlChannel::~ControlChannel() {
    stop();
}

// Connect and start the background reader thread.
void ControlChannel::start() {
    if (running_.load()) {
        return;
    }
    running_.store(true);
    reader_ = std::thread(&ControlChannel::read_loop, this);
}

// Stop the reader thread and close the socket.
void ControlChannel::stop() {
    running_.store(false);
    int sock = -1;
    {
        std::lock_guard<std::mutex> lock(write_mu_);
        sock = sock_;
        sock_ = -1;
    }
    if (sock >= 0) {
        // Wake the reader thread by shutting down the socket.
        shutdown(sock, SHUT_RDWR);
        close(sock);
    }
    if (reader_.joinable()) {
        reader_.join();
    }
}

// Send a JSON event to Python, one line per message.
void ControlChannel::send_event(const std::string &json_line) {
    std::lock_guard<std::mutex> lock(write_mu_);
    if (sock_ < 0 && running_.load()) {
        connect_socket_locked();
    }
    if (sock_ < 0) {
        return;
    }
    std::string line = json_line;
    if (line.empty() || line.back() != '\n') {
        line.push_back('\n');
    }
    size_t offset = 0;
    while (offset < line.size()) {
        ssize_t n = ::send(sock_,
                           line.data() + offset,
                           line.size() - offset,
                           0);
        if (n <= 0) {
            std::cerr << "control_send_failed errno=" << errno << std::endl;
            shutdown(sock_, SHUT_RDWR);
            close(sock_);
            sock_ = -1;
            return;
        }
        offset += static_cast<size_t>(n);
    }
}

// Establish the Unix domain socket connection to Python.
bool ControlChannel::connect_socket_locked() {
    if (sock_ >= 0) {
        return true;
    }

    int candidate = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (candidate < 0) {
        return false;
    }

    sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    // Validate path length for sockaddr_un.
    if (socket_path_.size() >= sizeof(addr.sun_path)) {
        close(candidate);
        return false;
    }
    std::strncpy(addr.sun_path, socket_path_.c_str(),
                 sizeof(addr.sun_path) - 1);

    // Connect to Python listener.
    if (connect(candidate, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) !=
        0) {
        close(candidate);
        return false;
    }
    sock_ = candidate;
    return true;
}

// Read newline-delimited JSON messages and dispatch control callbacks.
void ControlChannel::read_loop() {
    constexpr int kReconnectInitialMs = 200;
    constexpr int kReconnectMaxMs = 5000;
    int reconnect_sleep_ms = kReconnectInitialMs;

    std::string buffer;
    buffer.reserve(4096);
    std::vector<char> chunk(1024);

    while (running_.load()) {
        int local_sock = -1;
        {
            std::lock_guard<std::mutex> lock(write_mu_);
            if (sock_ < 0 && connect_socket_locked()) {
                reconnect_sleep_ms = kReconnectInitialMs;
                std::cerr << "control_connected path=" << socket_path_ << std::endl;
            }
            local_sock = sock_;
        }

        if (local_sock < 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(reconnect_sleep_ms));
            reconnect_sleep_ms =
                std::min(reconnect_sleep_ms * 2, kReconnectMaxMs);
            continue;
        }

        ssize_t n = ::recv(local_sock, chunk.data(), chunk.size(), 0);
        if (n <= 0) {
            bool disconnected = false;
            {
                std::lock_guard<std::mutex> lock(write_mu_);
                if (sock_ == local_sock) {
                    shutdown(sock_, SHUT_RDWR);
                    close(sock_);
                    sock_ = -1;
                    disconnected = true;
                }
            }
            if (disconnected && running_.load()) {
                std::cerr << "control_disconnected path=" << socket_path_ << std::endl;
            }
            buffer.clear();
            continue;
        }
        // Accumulate bytes and split on newline boundaries.
        buffer.append(chunk.data(), static_cast<size_t>(n));
        size_t pos = 0;
        while (true) {
            size_t newline = buffer.find('\n', pos);
            if (newline == std::string::npos) {
                buffer.erase(0, pos);
                break;
            }
            std::string line = buffer.substr(pos, newline - pos);
            pos = newline + 1;
            if (line.empty()) {
                continue;
            }
            std::unordered_map<std::string, std::string> fields;
            std::string parse_error;
            if (!ParseJsonObjectStringFields(line, fields, parse_error)) {
                std::cerr << "control_parse_failed error=" << parse_error
                          << " bytes=" << line.size() << std::endl;
                continue;
            }
            auto type = GetStringField(fields, "type");
            if (type == "call_ready") {
                auto session_id = GetStringField(fields, "session_id");
                auto seq = GetStringField(fields, "seq");
                if (!session_id.empty() && on_ready_) {
                    on_ready_(session_id);
                }
                if (!session_id.empty() && !seq.empty()) {
                    std::ostringstream ack;
                    ack << "{\"type\":\"control_ack\""
                        << ",\"command\":\"call_ready\""
                        << ",\"session_id\":\"" << JsonEscape(session_id) << "\""
                        << ",\"seq\":\"" << JsonEscape(seq) << "\""
                        << "}";
                    send_event(ack.str());
                }
                continue;
            }
            if (type == "call_hangup") {
                auto session_id = GetStringField(fields, "session_id");
                auto reason = GetStringField(fields, "reason");
                auto seq = GetStringField(fields, "seq");
                if (!session_id.empty() && on_hangup_) {
                    on_hangup_(session_id, reason);
                }
                if (!session_id.empty() && !seq.empty()) {
                    std::ostringstream ack;
                    ack << "{\"type\":\"control_ack\""
                        << ",\"command\":\"call_hangup\""
                        << ",\"session_id\":\"" << JsonEscape(session_id) << "\""
                        << ",\"seq\":\"" << JsonEscape(seq) << "\""
                        << "}";
                    send_event(ack.str());
                }
            }
        }
    }
}
