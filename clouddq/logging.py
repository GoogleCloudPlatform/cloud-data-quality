from datetime import datetime

import json
import logging
import sys


APP_VERSION = "0.4.0-rc1"
APP_NAME = "clouddq"
LOG_LEVEL = logging._nameToLevel["DEBUG"]


class JsonEncoderStrFallback(json.JSONEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError as exc:
            if "not JSON serializable" in str(exc):
                return str(obj)
            raise


class JsonEncoderDatetime(JsonEncoderStrFallback):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            return super().default(obj)


class JSONFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()

    def format(self, record):
        record.msg = json.dumps(
            {
                "severity": record.levelname,
                "time": datetime.utcfromtimestamp(record.created)
                .astimezone()
                .isoformat()
                .replace("+00:00", "Z"),
                "logging.googleapis.com/sourceLocation": {
                    "file": record.pathname or record.filename,
                    "function": record.funcName,
                    "line": record.lineno,
                },
                "exception": record.exc_info,
                "traceback": traceback.format_exception(*record.exc_info)
                if record.exc_info
                else None,
                "message": record.getMessage(),
                "logging.googleapis.com/labels": {
                    "name": APP_NAME,
                    "releaseId": APP_VERSION,
                },
            },
            cls=JsonEncoderDatetime,
        )
        return super().format(record)


def get_json_logger():
    json_logger = logging.getLogger("clouddq-json-logger")
    json_logger.setLevel(LOG_LEVEL)
    logging_stream_handler = logging.StreamHandler(sys.stderr)
    logging_stream_handler.setFormatter(JSONFormatter())
    json_logger.addHandler(logging_stream_handler)
    return json_logger


def get_logger():
    logger = logging.getLogger("clouddq")
    logger.setLevel(LOG_LEVEL)
    logging_stream_handler = logging.StreamHandler(sys.stderr)
    stream_formatter = logging.Formatter(
        "{asctime} {name} {levelname:8s} {message}", style="{"
    )
    logging_stream_handler.setFormatter(stream_formatter)
    logger.addHandler(logging_stream_handler)
    return logger
