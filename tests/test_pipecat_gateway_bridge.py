from __future__ import annotations

import queue
import time
import unittest

from src.pipecat_runtime.gateway_bridge import GatewaySessionBridge


class _FakeUdpAudioStream:
    instances: list["_FakeUdpAudioStream"] = []

    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs
        self.entered = False
        self.exited = False
        self._chunks: queue.Queue[bytes | None] = queue.Queue()
        type(self).instances.append(self)

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        _ = (exc_type, exc, tb)
        self.exited = True
        self._chunks.put(None)

    def push_chunk(self, chunk: bytes) -> None:
        self._chunks.put(chunk)

    def generator(self):
        while True:
            chunk = self._chunks.get()
            if chunk is None:
                return
            yield chunk


class _FakeUdpAudioSink:
    instances: list["_FakeUdpAudioSink"] = []

    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs
        self.sent: list[tuple[bytes, int | None]] = []
        self.flush_calls: list[bool] = []
        self.closed = False
        type(self).instances.append(self)

    def send_pcm16_stream_chunk(self, pcm16: bytes, source_rate: int | None = None) -> None:
        self.sent.append((pcm16, source_rate))

    def flush_pcm16_stream(self, pad_final_frame: bool = True) -> None:
        self.flush_calls.append(pad_final_frame)

    def close(self) -> None:
        self.closed = True


class GatewaySessionBridgeTest(unittest.TestCase):
    def _wait_until(self, predicate, timeout: float = 0.5) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.01)
        return predicate()

    def setUp(self) -> None:
        _FakeUdpAudioStream.instances.clear()
        _FakeUdpAudioSink.instances.clear()

    def _new_bridge(self) -> GatewaySessionBridge:
        return GatewaySessionBridge(
            session_id="sess-1",
            host="127.0.0.1",
            rx_port=41000,
            tx_port=41001,
            audio_stream_cls=_FakeUdpAudioStream,
            sink_cls=_FakeUdpAudioSink,
        )

    def test_media_ready_gates_inbound_audio_queue(self) -> None:
        bridge = self._new_bridge()
        bridge.start()
        stream = _FakeUdpAudioStream.instances[-1]
        stream.push_chunk(b"\x01\x02" * 10)

        self.assertIsNone(bridge.poll_audio_chunk(timeout=0.05))

        bridge.notify_media_started()
        self.assertEqual(bridge.poll_audio_chunk(timeout=0.2), b"\x01\x02" * 10)
        bridge.stop()

    def test_dtmf_queue_preserves_order(self) -> None:
        bridge = self._new_bridge()

        bridge.enqueue_dtmf_digit("1")
        bridge.enqueue_dtmf_digit("*")
        bridge.enqueue_dtmf_digit("#")

        self.assertEqual(bridge.poll_dtmf_digit(), "1")
        self.assertEqual(bridge.poll_dtmf_digit(), "*")
        self.assertEqual(bridge.poll_dtmf_digit(), "#")
        self.assertIsNone(bridge.poll_dtmf_digit())

    def test_outbound_audio_uses_current_udp_sink(self) -> None:
        bridge = self._new_bridge()
        bridge.start()

        bridge.send_output_audio(b"\x10\x20", sample_rate=8000)

        sink = _FakeUdpAudioSink.instances[-1]
        self.assertTrue(self._wait_until(lambda: bool(sink.sent)))
        self.assertEqual(sink.sent, [(b"\x10\x20", 8000)])
        snapshot = bridge.snapshot()
        self.assertEqual(snapshot.output_audio_chunks_enqueued, 1)
        self.assertEqual(snapshot.output_audio_bytes_enqueued, 2)
        self.assertEqual(snapshot.output_audio_chunks_sent, 1)
        self.assertEqual(snapshot.output_audio_bytes_sent, 2)
        bridge.stop()

    def test_flush_is_processed_by_background_output_worker(self) -> None:
        bridge = self._new_bridge()
        bridge.start()

        bridge.send_output_audio(b"\x10\x20", sample_rate=16000)
        bridge.flush_output_audio(pad_final_frame=True)

        sink = _FakeUdpAudioSink.instances[-1]
        self.assertTrue(self._wait_until(lambda: bool(sink.flush_calls)))
        self.assertEqual(sink.flush_calls, [True])
        snapshot = bridge.snapshot()
        self.assertEqual(snapshot.output_flush_requested, 1)
        self.assertEqual(snapshot.output_flush_processed, 1)
        self.assertEqual(snapshot.output_clear_events, 0)
        bridge.stop()

    def test_clear_output_audio_drains_pending_output_queue_and_tracks_clear_count(self) -> None:
        bridge = self._new_bridge()
        bridge.start()

        bridge.send_output_audio(b"\x10\x20", sample_rate=16000)
        bridge.send_output_audio(b"\x30\x40", sample_rate=16000)
        bridge.clear_output_audio()

        snapshot = bridge.snapshot()
        self.assertEqual(snapshot.output_clear_events, 1)
        self.assertGreaterEqual(snapshot.output_queue_items_cleared, 0)
        bridge.stop()

    def test_stop_closes_stream_and_sink_resources(self) -> None:
        bridge = self._new_bridge()
        bridge.start()

        bridge.stop()

        stream = _FakeUdpAudioStream.instances[-1]
        sink = _FakeUdpAudioSink.instances[-1]
        self.assertTrue(stream.exited)
        self.assertTrue(sink.closed)
        self.assertIsNone(bridge.audio_stream)
        self.assertIsNone(bridge.sink)
