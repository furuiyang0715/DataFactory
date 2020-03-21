import logging.config
import os


log_dir = os.path.dirname(__file__)


logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "simple": {
            "format": "[%(levelname)1.1s %(asctime)s|%(module)s|%(funcName)s|%(lineno)d] %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },
    "handlers": {
        "console": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": 'simple',
            "stream": "ext://sys.stdout"
        },
        "human_log_file": {
            "level": "INFO",
            "class": "logging.handlers.TimedRotatingFileHandler",
            "filename": os.path.join(log_dir, "human.log"),
            "formatter": "simple",
            "when": "D",
            "backupCount": 5
        },
    },
    "loggers": {
        "human_log": {
            "level": "DEBUG",
            "handlers": ["console", "human_log_file"]
        },
    }
})


logger = logging.getLogger("human_log")

# logger.debug("hello world1")
# logger.info("hello world2")
