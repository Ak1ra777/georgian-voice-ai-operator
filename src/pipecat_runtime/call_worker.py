from __future__ import annotations

import asyncio
import inspect
import queue
import threading
from typing import Callable

from pipecat.pipeline.runner import PipelineRunner

from src.config import (
    CALL_GREETING_AUDIO_PATH,
    CALL_GREETING_ENABLED,
    PIPECAT_ENABLE_PERIODIC_TELEMETRY,
    PIPECAT_TELEMETRY_INTERVAL_S,
)
from src.runtime.calls.session.runtime import SessionRuntime

from .artifacts import CallArtifactWriter
from .flow_runtime import snapshot_flow_state
from .gateway_bridge import GatewaySessionBridge
from .pipeline_factory import GatewayPipelineArtifacts, build_gateway_pipeline
from .policy import SessionPolicyController
from .telemetry import log_pipecat_call_snapshot


class PipecatCallWorker:
    """Compatibility worker that keeps Pipecat behind the current manager seam."""

    def __init__(
        self,
        session_id: str,
        rx_port: int,
        tx_port: int,
        remote_uri: str | None = None,
        host: str = "127.0.0.1",
        no_speech_timeout_s: float | None = None,
        max_duration_s: float | None = None,
        on_hangup_request: Callable[[str, str], None] | None = None,
        runtime: SessionRuntime | None = None,
        bridge_factory: Callable[..., GatewaySessionBridge] | None = None,
        greeting_enabled: bool = CALL_GREETING_ENABLED,
        greeting_audio_path: str | None = None,
        pipeline_builder: Callable[..., GatewayPipelineArtifacts] | None = None,
        artifact_writer_factory: Callable[..., CallArtifactWriter] | None = None,
        telemetry_enabled: bool = PIPECAT_ENABLE_PERIODIC_TELEMETRY,
        telemetry_interval_s: float = PIPECAT_TELEMETRY_INTERVAL_S,
    ) -> None:
        self.session_id = session_id
        self.rx_port = rx_port
        self.tx_port = tx_port
        self.remote_uri = str(remote_uri).strip() or None if remote_uri is not None else None
        self.host = host
        self.no_speech_timeout_s = no_speech_timeout_s
        self.max_duration_s = max_duration_s
        self._on_hangup_request = on_hangup_request
        self._bridge_factory = bridge_factory or GatewaySessionBridge
        self._greeting_enabled = greeting_enabled
        self._greeting_audio_path = greeting_audio_path or CALL_GREETING_AUDIO_PATH
        self._pipeline_builder = pipeline_builder or build_gateway_pipeline
        self._artifact_writer_factory = artifact_writer_factory or CallArtifactWriter
        self._telemetry_enabled = telemetry_enabled
        self._telemetry_interval_s = max(0.5, float(telemetry_interval_s))

        if runtime is not None and runtime.session_id != session_id:
            raise ValueError(
                "PipecatCallWorker runtime session_id mismatch: "
                f"expected={session_id} actual={runtime.session_id}"
            )
        self._runtime = (
            runtime if runtime is not None else SessionRuntime(session_id=session_id)
        )
        self._stop_event = self._runtime.stop_event
        self._thread: threading.Thread | None = None
        self._thread_lock = threading.Lock()
        self._media_ready_event = threading.Event()
        self._hangup_notified = threading.Event()
        self._dtmf_queue: queue.Queue[str] = queue.Queue()

        self._bridge: GatewaySessionBridge | None = None
        self._pipeline_task = None
        self._call_artifact_writer: CallArtifactWriter | None = None
        self._latest_artifacts: GatewayPipelineArtifacts | None = None

    @property
    def runtime(self) -> SessionRuntime:
        return self._runtime

    @property
    def bridge(self) -> GatewaySessionBridge | None:
        return self._bridge

    @property
    def call_artifact_writer(self) -> CallArtifactWriter | None:
        return self._call_artifact_writer

    def start(self) -> None:
        with self._thread_lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._ensure_call_artifact_writer()
            if self._bridge is None:
                bridge_kwargs = {
                    "session_id": self.session_id,
                    "host": self.host,
                    "rx_port": self.rx_port,
                    "tx_port": self.tx_port,
                }
                try:
                    parameters = inspect.signature(self._bridge_factory).parameters
                except (TypeError, ValueError):
                    parameters = {}
                if "event_recorder" in parameters:
                    bridge_kwargs["event_recorder"] = self._record_bridge_event
                self._bridge = self._bridge_factory(
                    **bridge_kwargs,
                )
            if hasattr(self._bridge, "set_event_recorder"):
                self._bridge.set_event_recorder(self._record_bridge_event)
            self._bridge.start()
            self._runtime.begin()
            self._thread = threading.Thread(
                target=self._run,
                name=f"pipecat-call-worker:{self.session_id}",
                daemon=True,
            )
            self._thread.start()
            self.record_control_event(
                source="worker",
                event="worker_started",
                data={
                    "rx_port": self.rx_port,
                    "tx_port": self.tx_port,
                },
            )
        print(f"pipecat_call_worker_started session_id={self.session_id}")

    def stop(self) -> None:
        self._runtime.request_stop("worker_stop_requested")
        bridge = self._bridge
        if bridge is not None:
            bridge.stop()
        thread: threading.Thread | None
        with self._thread_lock:
            thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)
        self._runtime.mark_ended("worker_stopped")
        print(f"pipecat_call_worker_stopped session_id={self.session_id}")

    def notify_media_started(self) -> None:
        self._media_ready_event.set()
        if self._bridge is not None:
            self._bridge.notify_media_started()

    def wait_for_media_ready(self, timeout: float | None = None) -> bool:
        return self._media_ready_event.wait(timeout=timeout)

    def on_dtmf_digit(self, digit: str) -> None:
        normalized = str(digit)
        self._dtmf_queue.put(normalized)
        if self._bridge is not None:
            self._bridge.enqueue_dtmf_digit(normalized)

    def poll_dtmf_digit(self, timeout: float | None = None) -> str | None:
        try:
            if timeout is None:
                return self._dtmf_queue.get_nowait()
            return self._dtmf_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def _request_hangup(self, reason: str) -> None:
        self._runtime.request_stop(f"hangup_requested:{reason}")
        if self._hangup_notified.is_set():
            return
        self._hangup_notified.set()
        if self._on_hangup_request is None:
            return
        try:
            self._on_hangup_request(self.session_id, reason)
        except Exception as ex:
            self.record_control_event(
                source="worker",
                event="hangup_request_failed",
                level="error",
                data={
                    "reason": reason,
                    "error": str(ex),
                },
            )
            print(
                "pipecat_call_worker_hangup_request_failed "
                f"session_id={self.session_id} error={ex}"
            )

    def _run(self) -> None:
        try:
            asyncio.run(self._run_async())
        except Exception as ex:
            self.record_control_event(
                source="worker",
                event="worker_failed",
                level="error",
                data={
                    "error": str(ex),
                },
            )
            print(
                "pipecat_call_worker_failed "
                f"session_id={self.session_id} error={ex}"
            )
            if not self._stop_event.is_set():
                self._request_hangup("worker_error")
        finally:
            self._runtime.mark_ended("worker_thread_finished")
            self._finalize_call_artifacts()

    async def _run_async(self) -> None:
        artifact_writer = self._ensure_call_artifact_writer()
        if artifact_writer is None:
            raise RuntimeError(
                f"PipecatCallWorker artifact writer unavailable for session {self.session_id}"
            )
        artifacts: GatewayPipelineArtifacts | None = None
        self._latest_artifacts = None
        bridge: GatewaySessionBridge | None = None
        cancel_task = None
        ingress_task = None
        telemetry_task = None
        policy_task = None

        try:
            media_ready = await asyncio.to_thread(self._wait_for_media_or_stop)
            if not media_ready or self._stop_event.is_set():
                return

            bridge = self._bridge
            if bridge is None:
                raise RuntimeError(
                    f"PipecatCallWorker bridge missing for session {self.session_id}"
                )

            policy_controller = SessionPolicyController(
                session_id=self.session_id,
                runtime=self._runtime,
                no_speech_timeout_s=self.no_speech_timeout_s,
                max_duration_s=self.max_duration_s,
            )
            policy_controller.mark_media_ready()

            artifacts = self._pipeline_builder(
                session_id=self.session_id,
                bridge=bridge,
                policy_controller=policy_controller,
                caller_uri=self.remote_uri,
                call_artifact_writer=artifact_writer,
                greeting_audio_path=(
                    self._greeting_audio_path if self._greeting_enabled else None
                ),
                request_hangup_fn=lambda reason: self._request_hangup(reason),
            )
            if artifacts.turn_timing is not None:
                artifacts.turn_timing.set_turn_archived_callback(
                    lambda archived: self._runtime.record_turn(spoke=archived.spoke)
                )
            self._latest_artifacts = artifacts
            self._pipeline_task = artifacts.task
            runner = PipelineRunner(
                handle_sigint=False,
                handle_sigterm=False,
            )
            runner_task = asyncio.create_task(runner.run(artifacts.task))
            cancel_task = asyncio.create_task(
                self._cancel_pipeline_when_stopped(artifacts.task)
            )
            ingress_task = asyncio.create_task(
                self._pump_bridge_inputs(artifacts.ingress, artifacts.task)
            )
            if self._telemetry_enabled:
                telemetry_task = asyncio.create_task(
                    self._emit_periodic_telemetry(bridge, artifacts)
                )
            policy_task = asyncio.create_task(
                policy_controller.monitor(
                    request_hangup_fn=lambda reason: self._request_hangup(reason),
                    should_stop_fn=self._stop_event.is_set,
                )
            )
            log_pipecat_call_snapshot(
                session_id=self.session_id,
                bridge=bridge,
                egress=artifacts.egress,
                turn_timing=artifacts.turn_timing,
                tool_runtime=artifacts.tool_runtime,
                runtime_snapshot=self._runtime.snapshot(),
                event="pipeline_started",
                media_event_recorder=self._record_media_event,
                tool_event_recorder=self._record_tool_event,
            )

            try:
                if artifacts.greeting_pcm16 is not None:
                    artifact_writer.append_assistant_audio(
                        artifacts.greeting_pcm16,
                        sample_rate=16000,
                    )
                    bridge.send_output_audio(
                        artifacts.greeting_pcm16,
                        sample_rate=16000,
                    )
                    bridge.flush_output_audio(pad_final_frame=True)
                    self.record_media_event(
                        source="worker",
                        event="greeting_sent",
                        data={
                            "bytes": len(artifacts.greeting_pcm16),
                        },
                    )
                    print(
                        "pipecat_greeting_sent "
                        f"session_id={self.session_id} bytes={len(artifacts.greeting_pcm16)}"
                    )
                if artifacts.flow_runtime is not None:
                    await artifacts.flow_runtime.manager.initialize(
                        artifacts.flow_runtime.initial_node
                    )
                await runner_task
                if not self._stop_event.is_set():
                    self._request_hangup("pipeline_stopped")
            finally:
                if cancel_task is not None:
                    cancel_task.cancel()
                if ingress_task is not None:
                    ingress_task.cancel()
                if telemetry_task is not None:
                    telemetry_task.cancel()
                if policy_task is not None:
                    policy_task.cancel()
                tasks = [
                    task
                    for task in (
                        cancel_task,
                        ingress_task,
                        policy_task,
                        telemetry_task,
                    )
                    if task is not None
                ]
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                log_pipecat_call_snapshot(
                    session_id=self.session_id,
                    bridge=bridge,
                    egress=artifacts.egress,
                    turn_timing=artifacts.turn_timing,
                    tool_runtime=artifacts.tool_runtime,
                    runtime_snapshot=self._runtime.snapshot(),
                    event="final",
                    media_event_recorder=self._record_media_event,
                    tool_event_recorder=self._record_tool_event,
                )
        finally:
            self._pipeline_task = None

    def _wait_for_media_or_stop(self) -> bool:
        while not self._stop_event.is_set():
            if self._media_ready_event.wait(timeout=0.05):
                return True
        return False

    async def _cancel_pipeline_when_stopped(self, task) -> None:
        await asyncio.to_thread(self._stop_event.wait)
        if not task.has_finished():
            await task.cancel(reason="worker_stop_requested")

    async def _pump_bridge_inputs(self, ingress, task) -> None:
        while not self._stop_event.is_set() and not task.has_finished():
            frames = await asyncio.to_thread(
                ingress.drain_ready_frames,
                audio_timeout_s=0.1,
            )
            for frame in frames:
                if self._stop_event.is_set() or task.has_finished():
                    return
                await task.queue_frame(frame)

    async def _emit_periodic_telemetry(
        self,
        bridge: GatewaySessionBridge,
        artifacts: GatewayPipelineArtifacts,
    ) -> None:
        try:
            while not self._stop_event.is_set() and not artifacts.task.has_finished():
                await asyncio.sleep(self._telemetry_interval_s)
                if self._stop_event.is_set() or artifacts.task.has_finished():
                    return
                log_pipecat_call_snapshot(
                    session_id=self.session_id,
                    bridge=bridge,
                    egress=artifacts.egress,
                    turn_timing=artifacts.turn_timing,
                    tool_runtime=artifacts.tool_runtime,
                    runtime_snapshot=self._runtime.snapshot(),
                    event="periodic",
                    media_event_recorder=self._record_media_event,
                    tool_event_recorder=self._record_tool_event,
                )
        except asyncio.CancelledError:
            return

    def _finalize_call_artifacts(self) -> None:
        artifact_writer = self._call_artifact_writer
        if artifact_writer is None:
            return
        artifacts = self._latest_artifacts
        bridge_snapshot = None
        if self._bridge is not None:
            snapshot_fn = getattr(self._bridge, "snapshot", None)
            if callable(snapshot_fn):
                bridge_snapshot = snapshot_fn()
        artifact_writer.finalize(
            runtime_snapshot=self._runtime.snapshot(),
            flow_state=self._build_flow_artifact_state(artifacts),
            tool_state=(
                artifacts.tool_runtime.artifact_state()
                if artifacts is not None and artifacts.tool_runtime is not None
                else None
            ),
            latency_state=(
                artifacts.turn_timing.snapshot()
                if artifacts is not None and artifacts.turn_timing is not None
                else None
            ),
            bridge_state=bridge_snapshot,
            egress_state=(
                artifacts.egress.snapshot()
                if artifacts is not None and artifacts.egress is not None
                else None
            ),
            tool_snapshot=(
                artifacts.tool_runtime.snapshot()
                if artifacts is not None and artifacts.tool_runtime is not None
                else None
            ),
        )

    def record_control_event(
        self,
        *,
        source: str,
        event: str,
        level: str = "info",
        timestamp: str | None = None,
        ts_ms: int | None = None,
        call_id: int | None = None,
        turn_id: int | None = None,
        seq: str | None = None,
        data: dict[str, object] | None = None,
    ) -> None:
        if source == "gateway" and event == "call_end":
            reason = (
                str((data or {}).get("reason", "")).strip()
                if isinstance(data, dict)
                else ""
            )
            if reason:
                self._runtime.request_stop(f"gateway_end:{reason}")
        artifact_writer = self._ensure_call_artifact_writer()
        if artifact_writer is None:
            return
        artifact_writer.record_control_event(
            source=source,
            event=event,
            level=level,
            timestamp=timestamp,
            ts_ms=ts_ms,
            call_id=call_id,
            turn_id=turn_id,
            seq=seq,
            data=data,
        )

    def record_media_event(
        self,
        *,
        source: str,
        event: str,
        level: str = "info",
        timestamp: str | None = None,
        ts_ms: int | None = None,
        call_id: int | None = None,
        turn_id: int | None = None,
        data: dict[str, object] | None = None,
    ) -> None:
        artifact_writer = self._ensure_call_artifact_writer()
        if artifact_writer is None:
            return
        artifact_writer.record_media_event(
            source=source,
            event=event,
            level=level,
            timestamp=timestamp,
            ts_ms=ts_ms,
            call_id=call_id,
            turn_id=turn_id,
            data=data,
        )

    def record_gateway_trace(
        self,
        *,
        event: str,
        level: str = "info",
        timestamp: str | None = None,
        ts_ms: int | None = None,
        call_id: int | None = None,
        data: dict[str, object] | None = None,
    ) -> None:
        artifact_writer = self._ensure_call_artifact_writer()
        if artifact_writer is None:
            return
        artifact_writer.record_gateway_trace(
            event=event,
            level=level,
            timestamp=timestamp,
            ts_ms=ts_ms,
            call_id=call_id,
            data=data,
        )

    def _record_bridge_event(self, event: str, data: dict[str, object]) -> None:
        self.record_media_event(source="bridge", event=event, data=data)

    def _record_media_event(self, event: str, data: dict[str, object]) -> None:
        self.record_media_event(source="worker", event=event, data=data)

    def _record_tool_event(self, event: str, data: dict[str, object]) -> None:
        level = str(data.get("level", "info")).strip() or "info"
        payload = {
            key: value
            for key, value in data.items()
            if key != "level"
        }
        artifact_writer = self._ensure_call_artifact_writer()
        if artifact_writer is None:
            return
        artifact_writer.record_tool_event(
            source="tools",
            event=event,
            level=level,
            data=payload,
        )

    def _ensure_call_artifact_writer(self) -> CallArtifactWriter | None:
        if self._call_artifact_writer is not None:
            return self._call_artifact_writer
        self._call_artifact_writer = self._artifact_writer_factory(
            session_id=self.session_id,
            caller_uri=self.remote_uri,
        )
        return self._call_artifact_writer

    @staticmethod
    def _build_flow_artifact_state(
        artifacts: GatewayPipelineArtifacts | None,
    ) -> dict[str, str | None]:
        if artifacts is None:
            return {
                "current_node": None,
                "branch": None,
                "active_dtmf_kind": None,
                "last_tool_id": None,
            }

        manager = artifacts.flow_runtime.manager if artifacts.flow_runtime is not None else None
        return snapshot_flow_state(
            manager=manager,
            dtmf_collector=artifacts.dtmf_collector,
        )
