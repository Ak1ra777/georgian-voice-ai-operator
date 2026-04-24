from __future__ import annotations

import queue
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable

from src.runtime.media.audio.udp_audio import UdpAudioSink, UdpAudioStream


@dataclass(frozen=True)
class _QueuedOutputAudio:
    pcm16: bytes
    sample_rate: int


@dataclass(frozen=True)
class _QueuedOutputFlush:
    pad_final_frame: bool


@dataclass(frozen=True)
class GatewaySessionBridgeSnapshot:
    session_id: str
    media_ready: bool
    audio_queue_depth: int
    audio_queue_high_watermark: int
    inbound_audio_chunks: int
    inbound_audio_bytes: int
    inbound_audio_drops: int
    output_queue_depth: int
    output_queue_high_watermark: int
    output_audio_chunks_enqueued: int
    output_audio_bytes_enqueued: int
    output_audio_chunks_sent: int
    output_audio_bytes_sent: int
    output_flush_requested: int
    output_flush_processed: int
    output_clear_events: int
    output_queue_items_cleared: int
    output_queue_drops: int


class GatewaySessionBridge:
    """Compatibility layer for the current UDP audio and control seam."""

    def __init__(
        self,
        *,
        session_id: str,
        host: str,
        rx_port: int,
        tx_port: int,
        audio_stream_cls=UdpAudioStream,
        sink_cls=UdpAudioSink,
        audio_queue_max: int = 200,
        output_queue_max: int = 200,
        event_recorder: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self.session_id = session_id
        self.host = host
        self.rx_port = rx_port
        self.tx_port = tx_port
        self._audio_stream_cls = audio_stream_cls
        self._sink_cls = sink_cls
        self._audio_queue: queue.Queue[bytes | None] = queue.Queue(
            maxsize=audio_queue_max
        )
        self._output_queue: queue.Queue[
            _QueuedOutputAudio | _QueuedOutputFlush | None
        ] = queue.Queue(maxsize=output_queue_max)
        self._dtmf_queue: queue.Queue[str] = queue.Queue()
        self._media_ready_event = threading.Event()
        self._stop_event = threading.Event()
        self._state_lock = threading.Lock()
        self._stats_lock = threading.Lock()
        self._event_recorder = event_recorder
        self._stream_context = None
        self._audio_stream = None
        self._sink = None
        self._last_audio_stream_stats: dict[str, Any] | None = None
        self._last_sink_stats: dict[str, Any] | None = None
        self._ingress_thread: threading.Thread | None = None
        self._egress_thread: threading.Thread | None = None
        self._audio_queue_high_watermark = 0
        self._inbound_audio_chunks = 0
        self._inbound_audio_bytes = 0
        self._inbound_audio_drops = 0
        self._output_queue_high_watermark = 0
        self._output_audio_chunks_enqueued = 0
        self._output_audio_bytes_enqueued = 0
        self._output_audio_chunks_sent = 0
        self._output_audio_bytes_sent = 0
        self._output_flush_requested = 0
        self._output_flush_processed = 0
        self._output_clear_events = 0
        self._output_queue_items_cleared = 0
        self._output_queue_drops = 0

    @property
    def audio_stream(self):
        return self._audio_stream

    @property
    def sink(self):
        return self._sink

    def start(self) -> "GatewaySessionBridge":
        with self._state_lock:
            if self._ingress_thread is not None and self._ingress_thread.is_alive():
                return self

            stream = self._audio_stream_cls(
                host=self.host,
                port=self.rx_port,
                fill_silence=False,
                name=f"pipecat_bridge_rx:{self.session_id}",
            )
            self._stream_context = stream
            if hasattr(stream, "__enter__"):
                self._audio_stream = stream.__enter__()
            else:
                self._audio_stream = stream

            self._sink = self._sink_cls(
                host=self.host,
                port=self.tx_port,
                name=f"pipecat_bridge_tx:{self.session_id}",
            )
            self._last_audio_stream_stats = None
            self._last_sink_stats = None
            self._stop_event.clear()
            self._egress_thread = threading.Thread(
                target=self._pump_outbound_audio,
                name=f"gateway-session-bridge-egress:{self.session_id}",
                daemon=True,
            )
            self._egress_thread.start()
            self._ingress_thread = threading.Thread(
                target=self._pump_inbound_audio,
                name=f"gateway-session-bridge:{self.session_id}",
                daemon=True,
            )
            self._ingress_thread.start()
        return self

    def stop(self) -> None:
        self._stop_event.set()
        stream_context = self._stream_context
        self._stream_context = None
        if stream_context is not None and hasattr(stream_context, "__exit__"):
            stream_context.__exit__(None, None, None)
        self._last_audio_stream_stats = self.audio_stream_stats()

        thread = self._ingress_thread
        if thread is not None:
            thread.join(timeout=1.0)
        self._ingress_thread = None

        try:
            self._output_queue.put_nowait(None)
        except queue.Full:
            pass
        egress_thread = self._egress_thread
        if egress_thread is not None:
            egress_thread.join(timeout=1.0)
        self._egress_thread = None

        sink = self._sink
        self._sink = None
        if sink is not None:
            sink.close()
            self._last_sink_stats = self._copy_stats(sink)

        self._audio_stream = None
        self._drain_queue(self._audio_queue)
        self._drain_queue(self._output_queue)
        self._drain_queue(self._dtmf_queue)
        try:
            self._audio_queue.put_nowait(None)
        except queue.Full:
            pass

    def notify_media_started(self) -> None:
        self._media_ready_event.set()

    def wait_for_media_ready(self, timeout: float | None = None) -> bool:
        return self._media_ready_event.wait(timeout=timeout)

    def enqueue_dtmf_digit(self, digit: str) -> None:
        self._dtmf_queue.put(str(digit))

    def set_event_recorder(
        self,
        event_recorder: Callable[[str, dict[str, Any]], None] | None,
    ) -> None:
        self._event_recorder = event_recorder

    def poll_dtmf_digit(self, timeout: float | None = None) -> str | None:
        return self._poll_queue(self._dtmf_queue, timeout=timeout)

    def poll_audio_chunk(self, timeout: float | None = None) -> bytes | None:
        return self._poll_queue(self._audio_queue, timeout=timeout)

    def send_output_audio(self, pcm16: bytes, *, sample_rate: int = 16000) -> None:
        if self._sink is None:
            raise RuntimeError(
                f"GatewaySessionBridge sink unavailable for session {self.session_id}"
            )
        self._queue_output_item(
            _QueuedOutputAudio(pcm16=pcm16, sample_rate=sample_rate)
        )

    def flush_output_audio(self, pad_final_frame: bool = True) -> None:
        if self._sink is None:
            return
        self._queue_output_item(_QueuedOutputFlush(pad_final_frame=pad_final_frame))

    def clear_output_audio(self) -> None:
        sink = self._sink
        if sink is None:
            return
        cleared_items = self._drain_queue(self._output_queue)
        with self._stats_lock:
            self._output_clear_events += 1
            self._output_queue_items_cleared += cleared_items
        sink.flush_pcm16_stream(pad_final_frame=False)

    def snapshot(self) -> GatewaySessionBridgeSnapshot:
        with self._stats_lock:
            return GatewaySessionBridgeSnapshot(
                session_id=self.session_id,
                media_ready=self._media_ready_event.is_set(),
                audio_queue_depth=self._audio_queue.qsize(),
                audio_queue_high_watermark=self._audio_queue_high_watermark,
                inbound_audio_chunks=self._inbound_audio_chunks,
                inbound_audio_bytes=self._inbound_audio_bytes,
                inbound_audio_drops=self._inbound_audio_drops,
                output_queue_depth=self._output_queue.qsize(),
                output_queue_high_watermark=self._output_queue_high_watermark,
                output_audio_chunks_enqueued=self._output_audio_chunks_enqueued,
                output_audio_bytes_enqueued=self._output_audio_bytes_enqueued,
                output_audio_chunks_sent=self._output_audio_chunks_sent,
                output_audio_bytes_sent=self._output_audio_bytes_sent,
                output_flush_requested=self._output_flush_requested,
                output_flush_processed=self._output_flush_processed,
                output_clear_events=self._output_clear_events,
                output_queue_items_cleared=self._output_queue_items_cleared,
                output_queue_drops=self._output_queue_drops,
            )

    def audio_stream_stats(self) -> dict[str, Any] | None:
        if self._audio_stream is not None:
            return self._copy_stats(self._audio_stream)
        if self._last_audio_stream_stats is None:
            return None
        return dict(self._last_audio_stream_stats)

    def sink_stats(self) -> dict[str, Any] | None:
        if self._sink is not None:
            return self._copy_stats(self._sink)
        if self._last_sink_stats is None:
            return None
        return dict(self._last_sink_stats)

    def _pump_inbound_audio(self) -> None:
        stream = self._audio_stream
        if stream is None:
            return

        while not self._stop_event.is_set():
            if self._media_ready_event.wait(timeout=0.05):
                break

        if self._stop_event.is_set():
            return

        for chunk in stream.generator():
            if self._stop_event.is_set():
                return
            try:
                self._audio_queue.put(chunk, timeout=0.1)
                queue_depth = self._audio_queue.qsize()
                with self._stats_lock:
                    self._inbound_audio_chunks += 1
                    self._inbound_audio_bytes += len(chunk)
                    if queue_depth > self._audio_queue_high_watermark:
                        self._audio_queue_high_watermark = queue_depth
            except queue.Full:
                with self._stats_lock:
                    self._inbound_audio_drops += 1
                self._record_event(
                    "audio_drop",
                    {
                        "reason": "queue_full",
                        "queue_depth": self._audio_queue.qsize(),
                    },
                )
                print(
                    "gateway_bridge_audio_drop "
                    f"session_id={self.session_id} reason=queue_full "
                    f"queue_depth={self._audio_queue.qsize()}"
                )

    def _pump_outbound_audio(self) -> None:
        while not self._stop_event.is_set():
            try:
                item = self._output_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            if item is None or self._stop_event.is_set():
                return

            sink = self._sink
            if sink is None:
                return

            if isinstance(item, _QueuedOutputAudio):
                sink.send_pcm16_stream_chunk(
                    item.pcm16,
                    source_rate=item.sample_rate,
                )
                with self._stats_lock:
                    self._output_audio_chunks_sent += 1
                    self._output_audio_bytes_sent += len(item.pcm16)
            elif isinstance(item, _QueuedOutputFlush):
                sink.flush_pcm16_stream(pad_final_frame=item.pad_final_frame)
                with self._stats_lock:
                    self._output_flush_processed += 1

    def _queue_output_item(
        self,
        item: _QueuedOutputAudio | _QueuedOutputFlush,
    ) -> None:
        try:
            self._output_queue.put(item, timeout=0.1)
            queue_depth = self._output_queue.qsize()
            with self._stats_lock:
                if queue_depth > self._output_queue_high_watermark:
                    self._output_queue_high_watermark = queue_depth
                if isinstance(item, _QueuedOutputAudio):
                    self._output_audio_chunks_enqueued += 1
                    self._output_audio_bytes_enqueued += len(item.pcm16)
                elif isinstance(item, _QueuedOutputFlush):
                    self._output_flush_requested += 1
        except queue.Full:
            with self._stats_lock:
                self._output_queue_drops += 1
            self._record_event(
                "output_drop",
                {
                    "reason": "queue_full",
                    "item_type": type(item).__name__,
                    "queue_depth": self._output_queue.qsize(),
                },
            )
            print(
                "gateway_bridge_output_drop "
                f"session_id={self.session_id} reason=queue_full "
                f"item_type={type(item).__name__} "
                f"queue_depth={self._output_queue.qsize()}"
            )

    def _record_event(self, event: str, data: dict[str, Any]) -> None:
        if self._event_recorder is None:
            return
        try:
            self._event_recorder(event, data)
        except Exception:
            return

    @staticmethod
    def _poll_queue(value_queue: queue.Queue, timeout: float | None = None):
        try:
            if timeout is None or timeout <= 0.0:
                return value_queue.get_nowait()
            return value_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    @staticmethod
    def _drain_queue(value_queue: queue.Queue) -> int:
        drained = 0
        while True:
            try:
                value_queue.get_nowait()
            except queue.Empty:
                return drained
            drained += 1

    @staticmethod
    def _copy_stats(component: object) -> dict[str, Any] | None:
        stats = getattr(component, "stats", None)
        if not callable(stats):
            return None
        payload = stats()
        if isinstance(payload, Mapping):
            return dict(payload)
        return None
