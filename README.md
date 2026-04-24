# Georgian Voice AI Operator
Georgian Voice AI Operator is a two-process SIP voice AI prototype for inbound telecom support calls. It connects a native SIP/RTP gateway to a Python Pipecat runtime, collects caller input through speech and DTMF, looks up ISP account context, routes support flows, and writes local call artifacts for audit and debugging.

## Problem Solved

Telecom support calls often need fast identity capture, account lookup, branch routing, and clean escalation notes before a human agent joins. This project demonstrates how those steps can be automated while keeping telephony, media transport, AI orchestration, and ISP business rules separated.

## Architecture

- `native/sip_gateway/`: C++ SIP gateway built on PJSUA2. It owns SIP registration, inbound call signaling, RTP media attachment, per-call UDP audio ports, and a Unix control socket.
- `sip_main.py`: Python manager entrypoint. It accepts gateway events, starts per-call workers, and coordinates session lifecycle.
- `src/pipecat_runtime/`: Pipecat pipeline assembly, processors, policy, telemetry, tool execution, transcript capture, and artifact writing.
- `src/runtime/`: lower-level call/session/media seams shared by the runtime.
- `src/tools/`: reusable tool contracts.
- `src/packs/isp/`: ISP-specific lookup, verification, ticketing, and flow rules.
- `tests/`: deterministic pytest coverage for protocol handling, call orchestration, Pipecat processors, ISP tools, and artifact output.

The gateway and Python runtime run separately. The C++ process handles SIP and media timing; the Python process handles AI, policy, business workflow, and generated artifacts.

## Features

- Inbound SIP call handling with a two-phase `call_allocated` / `call_ready` handshake.
- Raw PCM UDP bridge between native RTP handling and Python audio processors.
- Pipecat pipeline using OpenAI Responses, Google STT, and Cartesia TTS configuration seams.
- DTMF collection for subscriber identifiers, verification, and callback numbers.
- ISP support routing for technical support, billing, and human handoff.
- Local artifacts per call: transcripts, metadata, telemetry JSONL, ticket payloads, and optional WAV recordings.
- Unit tests that use fake providers and example-only customer data.

## Tech Stack

- Python 3.12
- Pipecat
- OpenAI Responses API
- Google Cloud Speech-to-Text
- Cartesia TTS
- C++17, PJSUA2 / pjproject
- pytest

## Setup

Create the Python environment:

```bash
python3.12 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Copy the sample configuration and fill in private values:

```bash
cp .env.example .env
```

Required values for a real call include:

- `OPENAI_API_KEY`
- `GOOGLE_APPLICATION_CREDENTIALS`
- `CARTESIA_API_KEY`
- `CARTESIA_TTS_VOICE_ID`
- `ISP_ACCOUNT_API_URL`
- `ISP_ACCOUNT_API_KEY`
- `SIP_DOMAIN`
- `SIP_USER`
- `SIP_PASS`

Build the native SIP gateway:

```bash
export PJPROJECT_DIR=/absolute/path/to/pjproject
make -C native/sip_gateway
```

Start the Python manager first:

```bash
set -a
source .env
set +a
.venv/bin/python sip_main.py
```

Start the SIP gateway in another terminal:

```bash
set -a
source .env
set +a
./native/sip_gateway/sip_gateway
```

The Python runtime loads `.env` directly. The C++ gateway reads environment variables from the shell, so values must be exported before launching it.

## Verification

Run the automated suite with:

```bash
.venv/bin/pytest -q
```

The tests are designed to run without live provider calls.

## Security Notes

This repository is prepared for portfolio publication. Real `.env` files, provider credentials, service account JSON, SIP credentials, generated call outputs, logs, local databases, and audio recordings are ignored. Runtime artifacts under `outputs/` may contain caller transcripts, tickets, telemetry, and recordings, so they should remain local unless intentionally scrubbed.

Checked-in audio files are excluded from the public snapshot. To use greeting audio locally, provide your own WAV file and set `CALL_GREETING_ENABLED=true` plus `CALL_GREETING_AUDIO_PATH=/path/to/file.wav`.

## Current Limitations

- End-to-end SIP calls require private SIP provider credentials and a local pjproject build.
- Provider-backed STT, TTS, and LLM behavior is configured but not exercised by the unit tests.
- The ISP lookup endpoint is an integration seam; public tests use stubs and fictional data.
- The project is a prototype, not a hardened production contact-center deployment.
