import json
import logging
import sys

import structlog


class PrettyJSON(str):
    def __repr__(self) -> str:
        # ConsoleRenderer uses repr(value); return raw text instead of quoted/escaped text
        return str(self)


def pretty_collections(_, __, event_dict):
    for key, value in list(event_dict.items()):
        if isinstance(value, (dict, list)):
            event_dict[key] = PrettyJSON(
                "\n" + json.dumps(value, indent=4, sort_keys=True, default=str)
            )
    return event_dict


def get_logger():
    log_stream = sys.stderr

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            pretty_collections,
            structlog.dev.ConsoleRenderer(colors=log_stream.isatty()),
        ],
        logger_factory=structlog.PrintLoggerFactory(file=log_stream),
    )

    return structlog.get_logger()


logger = get_logger()


def set_log_level(verbose: int) -> None:
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    level = levels[min(verbose, len(levels) - 1)]
    structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(level))
