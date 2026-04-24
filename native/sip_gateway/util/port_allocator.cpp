// Bind-check helper for reserving UDP ports.
#include "util/port_allocator.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>

namespace {
// Try to bind a UDP socket to the given port.
// Returns socket fd on success or -1 on failure.
int BindUdpSocket(int port) {
    int sock = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        return -1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(sock, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        close(sock);
        return -1;
    }
    return sock;
}
}  // namespace

// Construct a reservation with a pre-bound receive socket.
PortReservation::PortReservation(int rx, int tx, int recv)
    : rx_port(rx), tx_port(tx), recv_sock(recv) {}

// Close the reserved socket if still owned.
PortReservation::~PortReservation() {
    if (recv_sock >= 0) {
        close(recv_sock);
        recv_sock = -1;
    }
}

// Transfer the reserved socket to the caller and clear ownership.
int PortReservation::take_recv_sock() {
    int sock = recv_sock;
    recv_sock = -1;
    return sock;
}

// Initialize allocator with a fixed port range.
PortAllocator::PortAllocator(int base_port, int port_count)
    : base_port_(base_port),
      port_count_(port_count),
      next_index_(0) {}

// Reserve a port pair using round-robin selection and bind-check.
std::unique_ptr<PortReservation> PortAllocator::reserve_pair() {
    std::lock_guard<std::mutex> lock(mu_);

    if (port_count_ < 2) {
        return nullptr;
    }

    const int pair_count = port_count_ / 2;
    for (int i = 0; i < pair_count; ++i) {
        int index = (next_index_ + i) % pair_count;
        int rx_port = base_port_ + index * 2;
        int tx_port = rx_port + 1;

        // Skip ports already in use.
        if (in_use_.count(rx_port) || in_use_.count(tx_port)) {
            continue;
        }

        // Bind-check the receive port; if it fails, try the next pair.
        int recv_sock = BindUdpSocket(tx_port);
        if (recv_sock < 0) {
            continue;
        }

        in_use_.insert(rx_port);
        in_use_.insert(tx_port);
        next_index_ = (index + 1) % pair_count;
        return std::make_unique<PortReservation>(rx_port, tx_port, recv_sock);
    }

    return nullptr;
}

// Release a previously reserved port pair.
void PortAllocator::release_pair(const PortReservation &reservation) {
    std::lock_guard<std::mutex> lock(mu_);
    if (reservation.rx_port >= 0) {
        in_use_.erase(reservation.rx_port);
    }
    if (reservation.tx_port >= 0) {
        in_use_.erase(reservation.tx_port);
    }
}
