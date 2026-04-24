from __future__ import annotations

import unittest
from unittest.mock import patch

from src.runtime.media.audio.udp_audio import UdpAudioSink


class _FakeSocket:
    def __init__(self) -> None:
        self.sent_packets: list[tuple[bytes, tuple[str, int]]] = []
        self.closed = False

    def sendto(self, data: bytes, addr: tuple[str, int]) -> int:
        self.sent_packets.append((bytes(data), addr))
        return len(data)

    def close(self) -> None:
        self.closed = True


def _pcm16_samples(sample_count: int, sample_value: int = 1) -> bytes:
    sample = int(sample_value).to_bytes(2, byteorder="little", signed=True)
    return sample * sample_count


class UdpAudioStreamingTest(unittest.TestCase):
    def setUp(self) -> None:
        self._fake_sockets: list[_FakeSocket] = []

        def _new_socket(*args, **kwargs):
            _ = args, kwargs
            sock = _FakeSocket()
            self._fake_sockets.append(sock)
            return sock

        self._socket_patcher = patch(
            "src.runtime.media.audio.udp_audio.socket.socket",
            side_effect=_new_socket,
        )
        self._sleep_patcher = patch("src.runtime.media.audio.udp_audio.time.sleep", return_value=None)
        self._socket_patcher.start()
        self._sleep_patcher.start()
        self.addCleanup(self._socket_patcher.stop)
        self.addCleanup(self._sleep_patcher.stop)

    def _new_sink(self) -> tuple[UdpAudioSink, _FakeSocket]:
        sink = UdpAudioSink(
            host="127.0.0.1",
            port=41001,
            rate=16000,
            frame_ms=20,
            name="test_sink",
        )
        sock = self._fake_sockets[-1]
        return sink, sock

    def test_stream_chunking_and_flush_padding(self) -> None:
        sink, sock = self._new_sink()
        frame_bytes = sink.frame_bytes

        chunk_a = _pcm16_samples(100)  # 200 bytes
        chunk_b = _pcm16_samples(220)  # 440 bytes -> completes one frame
        chunk_c = _pcm16_samples(100)  # 200 bytes -> partial frame

        sink.send_pcm16_stream_chunk(chunk_a, source_rate=16000)
        self.assertEqual(len(sock.sent_packets), 0)

        sink.send_pcm16_stream_chunk(chunk_b, source_rate=16000)
        self.assertEqual(len(sock.sent_packets), 1)
        self.assertEqual(len(sock.sent_packets[0][0]), frame_bytes)

        sink.send_pcm16_stream_chunk(chunk_c, source_rate=16000)
        self.assertEqual(len(sock.sent_packets), 1)

        sink.flush_pcm16_stream(pad_final_frame=True)
        self.assertEqual(len(sock.sent_packets), 2)
        second_frame = sock.sent_packets[1][0]
        self.assertEqual(len(second_frame), frame_bytes)
        self.assertEqual(second_frame[: len(chunk_c)], chunk_c)
        self.assertEqual(second_frame[len(chunk_c) :], b"\x00" * (frame_bytes - len(chunk_c)))

        stats = sink.stats()
        self.assertEqual(stats["frames_sent_total"], 2)
        self.assertEqual(stats["stream_frames_sent_total"], 2)
        self.assertEqual(stats["stream_chunks_in"], 3)
        self.assertEqual(stats["stream_flushes"], 1)
        self.assertEqual(stats["partial_frames_padded"], 1)
        self.assertEqual(stats["stream_partial_frames_padded"], 1)

        sink.close()
        self.assertTrue(sock.closed)

    def test_flush_without_padding_drops_partial_frame(self) -> None:
        sink, sock = self._new_sink()

        sink.send_pcm16_stream_chunk(_pcm16_samples(120), source_rate=16000)  # 240 bytes
        self.assertEqual(len(sock.sent_packets), 0)

        sink.flush_pcm16_stream(pad_final_frame=False)
        self.assertEqual(len(sock.sent_packets), 0)

        stats = sink.stats()
        self.assertEqual(stats["stream_flushes"], 1)
        self.assertEqual(stats["partial_frames_padded"], 0)
        self.assertEqual(stats["stream_partial_frames_padded"], 0)

        sink.close()
        self.assertTrue(sock.closed)

    def test_resample_8k_input_produces_more_16k_frames(self) -> None:
        pcm_320_samples = _pcm16_samples(320)  # 640 bytes

        sink_same_rate, sock_same_rate = self._new_sink()
        sink_same_rate.send_pcm16_stream_chunk(pcm_320_samples, source_rate=16000)
        sink_same_rate.flush_pcm16_stream(pad_final_frame=True)
        frames_same_rate = len(sock_same_rate.sent_packets)
        sink_same_rate.close()

        sink_upsampled, sock_upsampled = self._new_sink()
        sink_upsampled.send_pcm16_stream_chunk(pcm_320_samples, source_rate=8000)
        sink_upsampled.flush_pcm16_stream(pad_final_frame=True)
        frames_upsampled = len(sock_upsampled.sent_packets)
        sink_upsampled.close()

        self.assertEqual(frames_same_rate, 1)
        self.assertGreaterEqual(frames_upsampled, 2)
        self.assertGreater(frames_upsampled, frames_same_rate)
        self.assertTrue(
            all(len(packet) == 640 for packet, _ in sock_upsampled.sent_packets),
            "All sent packets must stay 20ms PCM16 frames",
        )


if __name__ == "__main__":
    unittest.main()
