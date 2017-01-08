import logging
import logging.config

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '[%(asctime)s] [%(process)s] [%(levelname)s] %(name)s: %(message)s',  # noqa
            'datefmt': '%Y-%m-%d %H:%M:%S %z'
        },
    },
    'handlers': {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "standard",
            "stream": "ext://sys.stdout"
        },
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': True
        }
    }
})


def get_logger(name):
    return logging.getLogger(name)
