# Audio Package

## Purpose

`src/runtime/media/audio/` owns the raw PCM/UDP boundary used by the live call
worker.

This package is intentionally low-level. It handles audio format conversion,
UDP receive/send behavior, pacing, and transport-side observability for the
Python side of the SIP bridge.

## Responsibilities

- receive `16 kHz` PCM16 mono audio from the SIP gateway over UDP
- validate packet source and collect queue/jitter/packet-size stats
- rechunk incoming gateway packets into fixed frames for STT/VAD consumers
- send assistant audio back to the gateway as paced `20 ms` UDP frames
- provide small PCM/WAV helpers used by TTS and greeting playback

## Main Module

- `udp_audio.py`
  Contains the transport classes plus PCM conversion helpers.

## Mental Model

There are two different directions in this package:

- `UdpAudioStream`
  Gateway -> Python. A background receive thread reads UDP packets, optionally
  locks onto the first valid source, and exposes a generator of fixed-size
  PCM chunks for the STT listener.
- `UdpAudioSink`
  Python -> gateway. The sink accepts PCM chunks or a WAV file, optionally
  resamples/applies gain, and sends paced `20 ms` UDP packets back to the SIP
  media bridge.

Important fixed assumptions:

- the Python-side audio boundary is `16 kHz`, `PCM16`, `mono`
- gateway packets are usually `320` samples / `640` bytes (`20 ms`)
- STT/VAD consumption is usually `512` samples / `1024` bytes per yielded chunk

## Where It Is Used

- `src/runtime/calls/worker/resources.py`
  Builds `UdpAudioStream` for live inbound call audio.
- `src/runtime/media/tts/sip_tts.py`
  Uses `UdpAudioSink` for low-latency assistant playback back into the call.

## Rules

- Keep audio transport and sample-shape logic here.
- Keep STT session policy, VAD behavior, and TTS provider logic out of this
  package.
- Keep source validation, pacing, gain, and packet observability here.
