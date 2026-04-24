# Project Summary

## What Was Built

Demo Voice Assistant is a SIP-based voice AI support prototype for an internet service provider. It answers inbound calls through a native SIP gateway, streams caller audio into a Python Pipecat runtime, routes the caller through support flows, collects DTMF input when exact identifiers are needed, and writes structured artifacts such as transcripts, telemetry, and ticket payloads.

## Role

This project covers end-to-end system design and implementation across the telephony boundary, Python runtime orchestration, AI service integration, ISP workflow modeling, and automated tests. The work includes the native gateway contract, Python call manager, Pipecat pipeline assembly, tool contracts, ISP-specific support logic, and portfolio-safe repository hygiene.

## Technologies Used

- Python 3.12 for runtime orchestration, tools, tests, and artifacts.
- Pipecat for real-time voice pipeline composition.
- OpenAI Responses API integration for assistant reasoning.
- Google Cloud Speech-to-Text and Cartesia TTS configuration seams.
- C++17 with PJSUA2 / pjproject for SIP signaling and media bridging.
- Unix domain sockets and UDP PCM streams for process boundaries.
- pytest for deterministic unit and integration-boundary coverage.

## AI Engineering Relevance

The project demonstrates a practical voice AI architecture where model calls are not isolated demos: they are part of a real-time call lifecycle with media timing, turn-taking, interruptions, validation, tool execution, and durable call artifacts. The runtime keeps AI behavior behind explicit seams so tests can use fake providers while production can use live STT, LLM, and TTS services.

## Voice AI And Telecom Relevance

The system separates SIP/RTP responsibilities from AI orchestration. The native gateway manages registration, incoming calls, media attachment, and call cleanup, while Python handles conversational state and business workflow. This mirrors the boundary a production voice AI system needs when integrating modern model services with legacy telecom infrastructure.

## DevOps And Operations Relevance

Configuration is environment-driven, generated artifacts are isolated under `outputs/`, and tests are deterministic. The repository also documents which secrets and local outputs must stay out of source control, which is essential for publishing or deploying systems that touch call data and provider credentials.

## Support Automation Relevance

The ISP pack shows how a voice assistant can collect subscriber identifiers, load customer context, check technical status, route billing or technical issues, and produce escalation tickets. The public fixtures use example-only customer IDs, account IDs, service IDs, domains, and reserved phone numbers.
