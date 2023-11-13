import logging
import sys

def setup_logging(module=None, level=logging.INFO):  # pragma: no cover
    """Configures the given logger (or the root logger) to output to the supplied
    stream (or standard out) at the supplied logging level (or INFO).  Also
    configures all additional loggers."""
    logger = logging.getLogger(module or '')
    logger.setLevel(level)
    logging.Formatter.converter = time.gmtime
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(processName)s - %(levelname)s - %(message)s'
    )
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger