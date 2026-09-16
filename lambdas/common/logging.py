"""Structured JSON logging for AWS Lambda handlers.

Every entry carries the propagated trace_id (minted by the producer) so logs
can be correlated hop-to-hop (DATA_FLOW.md §8).

Handlers log structured context via `data=` on info/warning/error/debug:
    logger.warning("dropping record", data={"record_id": rid, "reason": exc})
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter for a single structured log line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        trace_id = _current_trace_id()
        if trace_id:
            payload["trace_id"] = trace_id
        extra = getattr(record, "data", None)
        if isinstance(extra, dict):
            payload.update(extra)
        return json.dumps(payload, default=str)


class StructuredLogger(logging.Logger):
    """Logger whose methods accept `data=` as structured context."""

    def _log_data(
        self,
        level: int,
        msg: object,
        args: tuple[Any, ...] = (),
        *,
        data: dict[str, Any] | None = None,
        exc_info: Any = None,
        stack_info: bool = False,
        **kwargs: Any,
    ) -> None:
        extra = dict(kwargs.pop("extra", {}) or {})
        if data is not None:
            extra["data"] = data
        super()._log(
            level, msg, args, exc_info=exc_info, stack_info=stack_info, extra=extra, **kwargs
        )

    def debug(
        self,
        msg: object,
        *args: Any,
        data: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._log_data(logging.DEBUG, msg, args, data=data, **kwargs)

    def info(
        self,
        msg: object,
        *args: Any,
        data: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._log_data(logging.INFO, msg, args, data=data, **kwargs)

    def warning(
        self,
        msg: object,
        *args: Any,
        data: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._log_data(logging.WARNING, msg, args, data=data, **kwargs)

    def error(
        self,
        msg: object,
        *args: Any,
        data: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._log_data(logging.ERROR, msg, args, data=data, **kwargs)


logging.setLoggerClass(StructuredLogger)


def _current_trace_id() -> str | None:
    """Return the active trace id if the runtime sets one per invocation."""
    return os.environ.get("_X_AMZN_TRACE_ID") or None


def get_logger(name: str) -> StructuredLogger:
    """Return a logger configured for JSON output (idempotent)."""
    logger = logging.getLogger(name)
    if not isinstance(logger, StructuredLogger):
        raise TypeError(f"custom logger class not installed: {type(logger).__name__}")
    logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.propagate = False
    return logger
