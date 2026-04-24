# Tests

## Purpose

`tests/` covers the Python runtime, dialog graph, media helpers, tool
contracts, and control/protocol seams.

## Coverage Areas

- dialog graph and nodes
  `test_graph_engine.py`, `test_function_node.py`, `test_component_node.py`,
  `test_collect_digits_node.py`, `test_logic_split_node.py`,
  `test_dialog_spec_compiler.py`, `test_dialog_spec_manifests.py`
- first-party ISP pack
  `test_isp_pack.py`, `test_isp_ticketing.py`, `test_agent_profile_runtime.py`
- runtime and worker orchestration
  `test_runtime_directives.py`, `test_session_runtime.py`,
  `test_worker_runtime_capabilities.py`, `test_call_worker_phase0.py`,
  `test_call_worker_timeout.py`, `test_stt_repeat_recovery.py`
- media, protocol, and speech helpers
  `test_udp_audio_streaming.py`, `test_pipeline_listen_once.py`,
  `test_pipeline_turn_deadline.py`, `test_silero_vad_loader.py`,
  `test_stt_turn_finalization.py`, `test_stt_repeat_recovery.py`,
  `test_tts_gate.py`, `test_dtmf_buffer.py`, `test_protocol_dtmf.py`,
  `test_filler_audio.py`, `test_control_protocol.py`
- provider/client wrappers
  `test_openai_client.py`, `test_cartesia_tts_security.py`
- load exercise
  `load_test_concurrent_calls.py`
- manual smoke utilities
  `manual_pipecat_control_smoke.py`

## Rules

- Prefer deterministic tests that do not require live provider calls.
- Add tests close to the layer being changed and keep each file focused on one
  ownership boundary.
- Keep manual smoke utilities out of pytest discovery unless they are made
  deterministic and provider-independent.
