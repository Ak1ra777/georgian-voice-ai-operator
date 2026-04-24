#pragma once

// Allocates per-call UDP port pairs with real bind-check.
// Keeps the receive socket open to reserve the port until used.

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>

// Represents a reserved (rx, tx) UDP port pair.
// recv_sock is kept open to prevent other processes from stealing the port.
struct PortReservation {
    int rx_port{-1};
    int tx_port{-1};
    int recv_sock{-1};

    // Create a reservation with already-bound recv socket.
    PortReservation(int rx, int tx, int recv);
    // Close the reserved socket if still held.
    ~PortReservation();

    // Transfer ownership of the reserved socket to the caller.
    int take_recv_sock();
};

class PortAllocator {
  public:
    // base_port + port_count define the allowed range.
    PortAllocator(int base_port, int port_count);

    // Reserve an (rx, tx) pair; returns nullptr if none available.
    std::unique_ptr<PortReservation> reserve_pair();
    // Release a previously reserved pair.
    void release_pair(const PortReservation &reservation);

  private:
    // Start of the reserved UDP port range.
    int base_port_;
    // Number of ports in the range (must be even).
    int port_count_;
    // Round-robin index for fairness.
    int next_index_;
    // Protects in_use_ and allocation state.
    std::mutex mu_;
    // Tracks ports currently reserved.
    std::unordered_set<int> in_use_;
};
