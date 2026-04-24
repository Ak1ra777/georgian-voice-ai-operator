import signal
import sys

from loguru import logger

from src.config import PIPECAT_LOG_LEVEL


def _configure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(line_buffering=True, write_through=True)


def _configure_logging() -> None:
    logger.remove()
    logger.add(sys.stderr, level=PIPECAT_LOG_LEVEL)


if __name__ == "__main__":
    _configure_stdio()
    _configure_logging()
    from src.pipecat_runtime.call_manager import PipecatCallManager
    from src.pipecat_runtime.validation import validate_pipecat_runtime

    validate_pipecat_runtime()
    manager = PipecatCallManager()

    def _handle_signal(signum, _frame) -> None:
        try:
            signal_name = signal.Signals(signum).name.lower()
        except Exception:
            signal_name = str(signum)
        manager.request_stop(f"signal_{signal_name}")

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    manager.run()
