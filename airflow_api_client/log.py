"""Logging configuration for the application, using structlog for structured logging with
pretty-printed collections in the console output.
"""

import json
import logging
import sys

import structlog

LOG_STREAM = sys.stderr


class PrettyJSON(str):
    """A string wrapper that is used to indicate that the string should be rendered as-is in the
    console renderer, without quotes or escaping."""

    def __repr__(self) -> str:
        # ConsoleRenderer uses repr(value); return raw text instead of quoted/escaped text
        return str(self)


def pretty_collections(_, __, event_dict):
    """A structlog processor that pretty-prints dicts and lists in the log output."""
    for key, value in list(event_dict.items()):
        if isinstance(value, (dict, list)):
            event_dict[key] = PrettyJSON(
                "\n" + json.dumps(value, indent=4, sort_keys=True, default=str)
            )
    return event_dict


def get_logger() -> structlog.BoundLogger:
    """Configure structlog to pretty-print logs to the console."""

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            pretty_collections,
            structlog.dev.ConsoleRenderer(colors=LOG_STREAM.isatty()),
        ],
        logger_factory=structlog.PrintLoggerFactory(file=LOG_STREAM),
    )

    return structlog.get_logger()


logger = get_logger()


def set_log_level(verbose: int) -> None:
    """Set the log level based on the verbosity level provided in the arguments."""
    levels = [logging.INFO, logging.DEBUG]
    level = levels[min(verbose, len(levels) - 1)]
    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(level))
    if level != logging.DEBUG:
        return

    # Add callsite information (filename, line number, function name) to the log output when in
    # DEBUG mode
    config = structlog.get_config()
    processors = list(config["processors"])
    if not any(isinstance(p, structlog.processors.CallsiteParameterAdder) for p in processors):
        processors.insert(
            0,
            structlog.processors.CallsiteParameterAdder(
                [
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.LINENO,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                ]
            ),
        )
        structlog.configure(
            processors=processors,
            wrapper_class=structlog.make_filtering_bound_logger(level),
            logger_factory=structlog.PrintLoggerFactory(file=LOG_STREAM),
        )
