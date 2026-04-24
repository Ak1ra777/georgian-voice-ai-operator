from __future__ import annotations

import math
import queue
import socket
import threading
import time
import wave
from typing import Optional

import numpy as np
from src.config import SIP_AUDIO_HOST, SIP_AUDIO_RX_PORT, SIP_AUDIO_TX_PORT


DEFAULT_RATE = 16000
DEFAULT_FRAME_MS = 20
DEFAULT_PACKET_SAMPLES = DEFAULT_RATE * DEFAULT_FRAME_MS // 1000
DEFAULT_PACKET_BYTES = DEFAULT_PACKET_SAMPLES * 2


class _LinearResampleState:
    """Streaming linear-resampler state for mono PCM16 audio."""

    def __init__(self) -> None:
        self.buffer = np.empty(0, dtype=np.float32)
        self.next_position = 0.0


def _pcm16_bytes_to_float32_samples(pcm16: bytes) -> np.ndarray:
    if not pcm16:
        return np.empty(0, dtype=np.float32)
    return np.frombuffer(pcm16, dtype=np.int16).astype(np.float32)


def _float32_samples_to_pcm16_bytes(samples: np.ndarray) -> bytes:
    if samples.size == 0:
        return b""
    return np.clip(samples, -32768.0, 32767.0).astype(np.int16).tobytes()


def _resample_pcm16_mono_linear(
    pcm16: bytes,
    *,
    src_rate: int,
    dst_rate: int,
    state: _LinearResampleState | None = None,
    finalize: bool = False,
) -> tuple[bytes, _LinearResampleState | None]:
    """Resample mono PCM16 audio with streaming-safe linear interpolation."""

    if not pcm16 and not finalize:
        return b"", state
    if src_rate <= 0 or dst_rate <= 0:
        raise ValueError(
            f"Invalid resample rates: src_rate={src_rate} dst_rate={dst_rate}"
        )
    if src_rate == dst_rate:
        return pcm16, None if finalize else state

    active_state = state or _LinearResampleState()
    incoming = _pcm16_bytes_to_float32_samples(pcm16)
    if incoming.size:
        if active_state.buffer.size == 0:
            active_state.buffer = incoming
        else:
            active_state.buffer = np.concatenate((active_state.buffer, incoming))

    if active_state.buffer.size == 0:
        if finalize:
            return b"", None
        return b"", active_state

    processing = active_state.buffer
    if finalize:
        processing = np.concatenate((processing, processing[-1:]))

    if processing.size < 2:
        if finalize:
            return b"", None
        return b"", active_state

    step = float(src_rate) / float(dst_rate)
    positions = np.arange(
        active_state.next_position,
        float(processing.size - 1),
        step,
        dtype=np.float64,
    )
    if positions.size == 0:
        if finalize:
            return b"", None
        return b"", active_state

    indices = np.floor(positions).astype(np.int64)
    fractions = positions - indices
    output = (
        processing[indices] * (1.0 - fractions)
        + processing[indices + 1] * fractions
    )
    next_position = float(positions[-1] + step)

    if finalize:
        return _float32_samples_to_pcm16_bytes(output), None

    drop_count = int(math.floor(next_position))
    if drop_count > 0:
        active_state.buffer = active_state.buffer[drop_count:]
        next_position -= drop_count
    active_state.next_position = next_position
    return _float32_samples_to_pcm16_bytes(output), active_state


def _to_int16(frames: bytes, sample_width: int) -> bytes:
    if sample_width == 2:
        return frames
    if sample_width == 1:
        data = np.frombuffer(frames, dtype=np.uint8).astype(np.int16)
        data = (data - 128) << 8
        return data.tobytes()
    if sample_width == 4:
        data = np.frombuffer(frames, dtype=np.float32)
        if np.max(np.abs(data)) > 1.0:
            data = data / 2147483648.0
        data = np.clip(data, -1.0, 1.0)
        return (data * 32767.0).astype(np.int16).tobytes()
    raise ValueError(f"Unsupported WAV sample width: {sample_width}")


def _downmix_mono(pcm16: bytes, channels: int) -> bytes:
    if channels == 1:
        return pcm16
    data = np.frombuffer(pcm16, dtype=np.int16)
    if data.size % channels:
        data = data[: data.size - (data.size % channels)]
    data = data.reshape(-1, channels)
    mono = data.mean(axis=1).astype(np.int16)
    return mono.tobytes()


def wav_to_pcm16_mono(path: str, target_rate: int = DEFAULT_RATE) -> bytes:
    with wave.open(path, "rb") as wav_file:
        rate = wav_file.getframerate()
        channels = wav_file.getnchannels()
        sample_width = wav_file.getsampwidth()
        frames = wav_file.readframes(wav_file.getnframes())

    pcm16 = _to_int16(frames, sample_width)
    pcm16 = _downmix_mono(pcm16, channels)

    if rate != target_rate:
        pcm16, _ = _resample_pcm16_mono_linear(
            pcm16,
            src_rate=rate,
            dst_rate=target_rate,
            finalize=True,
        )
    return pcm16


def normalize_pcm16_peak(
    pcm16: bytes, target_peak_dbfs: float = -3.0, max_gain_db: float = 14.0
) -> bytes:
    if not pcm16:
        return pcm16

    data = np.frombuffer(pcm16, dtype=np.int16).astype(np.float32)
    if data.size == 0:
        return pcm16

    peak = float(np.max(np.abs(data)))
    if peak <= 0.0:
        return pcm16

    target_peak = 32767.0 * (10.0 ** (target_peak_dbfs / 20.0))
    if target_peak <= 0.0:
        return pcm16

    max_gain = 10.0 ** (max_gain_db / 20.0)
    gain = min(target_peak / peak, max_gain)
    if gain <= 1.0:
        return pcm16

    boosted = np.clip(data * gain, -32768.0, 32767.0).astype(np.int16)
    return boosted.tobytes()


def apply_gain_pcm16(pcm16: bytes, gain_db: float) -> bytes:
    if not pcm16 or abs(gain_db) < 1e-6:
        return pcm16

    gain = 10.0 ** (gain_db / 20.0)
    data = np.frombuffer(pcm16, dtype=np.int16).astype(np.float32)
    if data.size == 0:
        return pcm16
    adjusted = np.clip(data * gain, -32768.0, 32767.0).astype(np.int16)
    return adjusted.tobytes()


class UdpAudioStream:
    """
    Receive 16k PCM16 mono audio over UDP and yield fixed-size frames.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        rate: int = DEFAULT_RATE,
        chunk: int = 512,
        packet_samples: int = DEFAULT_PACKET_SAMPLES,
        queue_max: int = 200,
        fill_silence: bool = True,
        name: Optional[str] = None,
        expected_source_host: Optional[str] = None,
        expected_source_port: Optional[int] = None,
        pin_source_on_first_packet: bool = True,
        jitter_gap_factor: float = 1.5,
    ) -> None:
        self.host = host or SIP_AUDIO_HOST
        self.port = port or SIP_AUDIO_RX_PORT
        self.rate = rate
        self.chunk = chunk
        self.packet_samples = packet_samples
        self.packet_bytes = packet_samples * 2
        self.chunk_bytes = chunk * 2
        self.fill_silence = fill_silence
        self.name = name or f"udp_stream:{self.host}:{self.port}"
        self.expected_source_host = (
            expected_source_host.strip() if expected_source_host else None
        )
        self.expected_source_port = (
            int(expected_source_port) if expected_source_port else None
        )
        if self.expected_source_port is not None and self.expected_source_port <= 0:
            self.expected_source_port = None
        self.pin_source_on_first_packet = bool(pin_source_on_first_packet)
        self.jitter_gap_factor = max(1.1, float(jitter_gap_factor))
        self._expected_packet_interval_s = self.packet_samples / float(
            max(1, self.rate)
        )
        self._recv_bytes = max(self.packet_bytes * 2, 2048)

        self._queue: queue.Queue[Optional[bytes]] = queue.Queue(maxsize=queue_max)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._sock: Optional[socket.socket] = None
        self._stats_mu = threading.Lock()
        self._packets_received = 0
        self._packets_received_bytes = 0
        self._packets_dropped_queue_full = 0
        self._packets_rejected_source = 0
        self._packets_rejected_source_host = 0
        self._packets_rejected_source_port = 0
        self._packets_rejected_source_pin = 0
        self._source_locked_host = ""
        self._source_locked_port = 0
        self._queue_high_watermark = 0
        self._packet_size_mismatch = 0
        self._packet_size_short = 0
        self._packet_size_long = 0
        self._packet_size_min = 0
        self._packet_size_max = 0
        self._inter_arrival_count = 0
        self._inter_arrival_ms_sum = 0.0
        self._inter_arrival_jitter_ms_sum = 0.0
        self._inter_arrival_jitter_ms_max = 0.0
        self._inter_arrival_gap_events = 0
        self._inter_arrival_burst_events = 0
        self._last_packet_arrival_ts: Optional[float] = None
        self._queue_timeouts = 0
        self._silence_frames_injected = 0
        self._chunks_yielded = 0

    def __enter__(self) -> "UdpAudioStream":
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, self.port))
        self._sock.settimeout(0.05)
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, type, value, traceback) -> None:
        self._stop_event.set()
        if self._sock is not None:
            self._sock.close()
        if self._thread is not None:
            self._thread.join(timeout=1.0)
        try:
            self._queue.put_nowait(None)
        except queue.Full:
            pass

    def stats(self) -> dict[str, int | float | str]:
        queue_depth = self._queue.qsize()
        with self._stats_mu:
            inter_arrival_ms_avg = 0.0
            inter_arrival_jitter_ms_avg = 0.0
            if self._inter_arrival_count > 0:
                inter_arrival_ms_avg = (
                    self._inter_arrival_ms_sum / self._inter_arrival_count
                )
                inter_arrival_jitter_ms_avg = (
                    self._inter_arrival_jitter_ms_sum / self._inter_arrival_count
                )
            return {
                "name": self.name,
                "packets_received": self._packets_received,
                "packets_received_bytes": self._packets_received_bytes,
                "packets_dropped_queue_full": self._packets_dropped_queue_full,
                "packets_rejected_source": self._packets_rejected_source,
                "packets_rejected_source_host": self._packets_rejected_source_host,
                "packets_rejected_source_port": self._packets_rejected_source_port,
                "packets_rejected_source_pin": self._packets_rejected_source_pin,
                "source_locked_host": self._source_locked_host,
                "source_locked_port": self._source_locked_port,
                "expected_source_host": self.expected_source_host or "",
                "expected_source_port": self.expected_source_port or 0,
                "queue_high_watermark": self._queue_high_watermark,
                "queue_depth": queue_depth,
                "packet_size_mismatch": self._packet_size_mismatch,
                "packet_size_short": self._packet_size_short,
                "packet_size_long": self._packet_size_long,
                "packet_size_min": self._packet_size_min,
                "packet_size_max": self._packet_size_max,
                "packet_interval_expected_ms": int(
                    self._expected_packet_interval_s * 1000.0
                ),
                "inter_arrival_count": self._inter_arrival_count,
                "inter_arrival_ms_avg": inter_arrival_ms_avg,
                "inter_arrival_jitter_ms_avg": inter_arrival_jitter_ms_avg,
                "inter_arrival_jitter_ms_max": self._inter_arrival_jitter_ms_max,
                "inter_arrival_gap_events": self._inter_arrival_gap_events,
                "inter_arrival_burst_events": self._inter_arrival_burst_events,
                "queue_timeouts": self._queue_timeouts,
                "silence_frames_injected": self._silence_frames_injected,
                "chunks_yielded": self._chunks_yielded,
            }

    def _recv_loop(self) -> None:
        assert self._sock is not None
        while not self._stop_event.is_set():
            try:
                data, source_addr = self._sock.recvfrom(self._recv_bytes)
            except socket.timeout:
                continue
            except OSError:
                break
            if not data:
                continue

            source_host = str(source_addr[0]) if source_addr else ""
            source_port = int(source_addr[1]) if source_addr else 0
            source_reject_reason = ""
            source_reject_total = 0
            source_locked = False
            packet_size_mismatch_total = 0
            packet_size_mismatch_kind = ""

            with self._stats_mu:
                if (
                    self.expected_source_host
                    and source_host != self.expected_source_host
                ):
                    self._packets_rejected_source += 1
                    self._packets_rejected_source_host += 1
                    source_reject_reason = "host"
                    source_reject_total = self._packets_rejected_source
                elif (
                    self.expected_source_port is not None
                    and source_port != self.expected_source_port
                ):
                    self._packets_rejected_source += 1
                    self._packets_rejected_source_port += 1
                    source_reject_reason = "port"
                    source_reject_total = self._packets_rejected_source
                elif self.pin_source_on_first_packet:
                    if not self._source_locked_host:
                        self._source_locked_host = source_host
                        self._source_locked_port = source_port
                        source_locked = True
                    elif (
                        source_host != self._source_locked_host
                        or source_port != self._source_locked_port
                    ):
                        self._packets_rejected_source += 1
                        self._packets_rejected_source_pin += 1
                        source_reject_reason = "pin"
                        source_reject_total = self._packets_rejected_source

                if not source_reject_reason:
                    now = time.monotonic()
                    if self._last_packet_arrival_ts is not None:
                        interval_s = now - self._last_packet_arrival_ts
                        if interval_s < 0:
                            interval_s = 0.0
                        interval_ms = interval_s * 1000.0
                        jitter_ms = abs(
                            interval_s - self._expected_packet_interval_s
                        ) * 1000.0
                        self._inter_arrival_count += 1
                        self._inter_arrival_ms_sum += interval_ms
                        self._inter_arrival_jitter_ms_sum += jitter_ms
                        if jitter_ms > self._inter_arrival_jitter_ms_max:
                            self._inter_arrival_jitter_ms_max = jitter_ms
                        if interval_s > (
                            self._expected_packet_interval_s
                            * self.jitter_gap_factor
                        ):
                            self._inter_arrival_gap_events += 1
                        elif interval_s < (
                            self._expected_packet_interval_s
                            / self.jitter_gap_factor
                        ):
                            self._inter_arrival_burst_events += 1
                    self._last_packet_arrival_ts = now

                    packet_size = len(data)
                    if (
                        self._packet_size_min == 0
                        or packet_size < self._packet_size_min
                    ):
                        self._packet_size_min = packet_size
                    if packet_size > self._packet_size_max:
                        self._packet_size_max = packet_size
                    if packet_size != self.packet_bytes:
                        self._packet_size_mismatch += 1
                        packet_size_mismatch_total = self._packet_size_mismatch
                        if packet_size < self.packet_bytes:
                            self._packet_size_short += 1
                            packet_size_mismatch_kind = "short"
                        else:
                            self._packet_size_long += 1
                            packet_size_mismatch_kind = "long"

                    self._packets_received += 1
                    self._packets_received_bytes += len(data)

            if source_locked:
                print(
                    f"udp_audio_rx_source_locked name={self.name} "
                    f"host={source_host} port={source_port}"
                )
            if source_reject_reason:
                if source_reject_total == 1 or source_reject_total % 100 == 0:
                    print(
                        f"udp_audio_rx_reject_source name={self.name} "
                        f"reason={source_reject_reason} "
                        f"host={source_host} port={source_port} "
                        f"rejected={source_reject_total}"
                    )
                continue

            if (
                packet_size_mismatch_total > 0
                and (
                    packet_size_mismatch_total == 1
                    or packet_size_mismatch_total % 100 == 0
                )
            ):
                print(
                    f"udp_audio_rx_packet_size_mismatch name={self.name} "
                    f"kind={packet_size_mismatch_kind} "
                    f"actual={len(data)} expected={self.packet_bytes} "
                    f"count={packet_size_mismatch_total}"
                )

            try:
                self._queue.put_nowait(data)
                queue_depth = self._queue.qsize()
                with self._stats_mu:
                    if queue_depth > self._queue_high_watermark:
                        self._queue_high_watermark = queue_depth
            except queue.Full:
                with self._stats_mu:
                    self._packets_dropped_queue_full += 1
                    drops = self._packets_dropped_queue_full
                if drops == 1 or drops % 100 == 0:
                    print(
                        f"udp_audio_rx_drop name={self.name} "
                        f"dropped={drops}"
                    )

    def generator(self):
        buffer = bytearray()
        frame_interval = self.chunk / float(self.rate)
        silence = b"\x00" * self.chunk_bytes

        while True:
            try:
                packet = self._queue.get(timeout=frame_interval)
            except queue.Empty:
                packet = None
                with self._stats_mu:
                    self._queue_timeouts += 1

            if packet is None:
                if self._stop_event.is_set():
                    return
                if self.fill_silence and not buffer:
                    with self._stats_mu:
                        self._silence_frames_injected += 1
                        self._chunks_yielded += 1
                    yield silence
                continue

            buffer.extend(packet)
            while len(buffer) >= self.chunk_bytes:
                chunk = bytes(buffer[: self.chunk_bytes])
                del buffer[: self.chunk_bytes]
                with self._stats_mu:
                    self._chunks_yielded += 1
                yield chunk


class UdpAudioSink:
    """
    Send 16k PCM16 mono audio over UDP in 20ms packets.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        rate: int = DEFAULT_RATE,
        frame_ms: int = DEFAULT_FRAME_MS,
        name: Optional[str] = None,
    ) -> None:
        self.host = host or SIP_AUDIO_HOST
        self.port = port or SIP_AUDIO_TX_PORT
        self.rate = rate
        self.frame_ms = frame_ms
        self.frame_samples = int(rate * frame_ms / 1000)
        self.frame_bytes = self.frame_samples * 2
        self.name = name or f"udp_sink:{self.host}:{self.port}"
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._addr = (self.host, self.port)
        self._stream_buffer = bytearray()
        self._stream_start_time: Optional[float] = None
        self._stream_frames_sent = 0
        self._stream_resample_state: _LinearResampleState | None = None
        self._stream_source_rate: Optional[int] = None
        self._stats_mu = threading.Lock()
        self._frames_sent_total = 0
        self._stream_frames_sent_total = 0
        self._stream_chunks_in = 0
        self._stream_flushes = 0
        self._partial_frames_padded = 0
        self._stream_partial_frames_padded = 0

    def close(self) -> None:
        self.flush_pcm16_stream(pad_final_frame=False)
        self._sock.close()

    def stats(self) -> dict[str, int | str]:
        with self._stats_mu:
            return {
                "name": self.name,
                "frames_sent_total": self._frames_sent_total,
                "stream_frames_sent_total": self._stream_frames_sent_total,
                "stream_chunks_in": self._stream_chunks_in,
                "stream_flushes": self._stream_flushes,
                "partial_frames_padded": self._partial_frames_padded,
                "stream_partial_frames_padded": self._stream_partial_frames_padded,
            }

    def _send_stream_frame(self, frame: bytes) -> None:
        if self._stream_start_time is None:
            self._stream_start_time = time.monotonic()
            self._stream_frames_sent = 0

        self._sock.sendto(frame, self._addr)
        self._stream_frames_sent += 1
        with self._stats_mu:
            self._frames_sent_total += 1
            self._stream_frames_sent_total += 1
        target = self._stream_start_time + (
            self._stream_frames_sent * (self.frame_samples / self.rate)
        )
        delay = target - time.monotonic()
        if delay > 0:
            time.sleep(delay)

    def _reset_stream_state(self) -> None:
        self._stream_buffer.clear()
        self._stream_start_time = None
        self._stream_frames_sent = 0
        self._stream_resample_state = None
        self._stream_source_rate = None

    def send_pcm16_stream_chunk(
        self, pcm16: bytes, source_rate: Optional[int] = None
    ) -> None:
        if not pcm16:
            return
        with self._stats_mu:
            self._stream_chunks_in += 1

        src_rate = source_rate or self.rate
        if src_rate != self.rate:
            if self._stream_source_rate is None:
                self._stream_source_rate = src_rate
            elif self._stream_source_rate != src_rate:
                self._stream_resample_state = None
                self._stream_source_rate = src_rate

            pcm16, self._stream_resample_state = _resample_pcm16_mono_linear(
                pcm16,
                src_rate=src_rate,
                dst_rate=self.rate,
                state=self._stream_resample_state,
            )
        else:
            self._stream_resample_state = None
            self._stream_source_rate = self.rate

        if not pcm16:
            return

        self._stream_buffer.extend(pcm16)
        while len(self._stream_buffer) >= self.frame_bytes:
            frame = bytes(self._stream_buffer[: self.frame_bytes])
            del self._stream_buffer[: self.frame_bytes]
            self._send_stream_frame(frame)

    def flush_pcm16_stream(self, pad_final_frame: bool = True) -> None:
        with self._stats_mu:
            self._stream_flushes += 1
        if (
            self._stream_source_rate is not None
            and self._stream_source_rate != self.rate
            and self._stream_resample_state is not None
        ):
            final_pcm16, _ = _resample_pcm16_mono_linear(
                b"",
                src_rate=self._stream_source_rate,
                dst_rate=self.rate,
                state=self._stream_resample_state,
                finalize=True,
            )
            if final_pcm16:
                self._stream_buffer.extend(final_pcm16)
        if self._stream_buffer:
            if pad_final_frame:
                frame = bytes(self._stream_buffer[: self.frame_bytes])
                if len(frame) < self.frame_bytes:
                    with self._stats_mu:
                        self._partial_frames_padded += 1
                        self._stream_partial_frames_padded += 1
                    frame = frame + b"\x00" * (self.frame_bytes - len(frame))
                self._send_stream_frame(frame)
        self._reset_stream_state()

    def send_wav(
        self,
        path: str,
        normalize_peak_dbfs: Optional[float] = None,
        max_gain_db: float = 14.0,
    ) -> None:
        pcm16 = wav_to_pcm16_mono(path, target_rate=self.rate)
        if normalize_peak_dbfs is not None:
            pcm16 = normalize_pcm16_peak(
                pcm16,
                target_peak_dbfs=normalize_peak_dbfs,
                max_gain_db=max_gain_db,
            )
        self.send_pcm16(pcm16)

    def send_pcm16(self, pcm16: bytes) -> None:
        if not pcm16:
            return
        total_frames = int(math.ceil(len(pcm16) / self.frame_bytes))
        start_time = time.monotonic()
        for i in range(total_frames):
            offset = i * self.frame_bytes
            chunk = pcm16[offset : offset + self.frame_bytes]
            if len(chunk) < self.frame_bytes:
                with self._stats_mu:
                    self._partial_frames_padded += 1
                chunk = chunk + b"\x00" * (self.frame_bytes - len(chunk))
            self._sock.sendto(chunk, self._addr)
            with self._stats_mu:
                self._frames_sent_total += 1
            target = start_time + (i + 1) * (self.frame_samples / self.rate)
            delay = target - time.monotonic()
            if delay > 0:
                time.sleep(delay)
