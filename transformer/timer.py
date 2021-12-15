import time

from loguru import logger


def timeit(method):
    """Decorator to time the execution of a function."""

    def timed(*args, **kw):
        start_time = time.time()
        logger.info(f"Starting execution of {method.__name__}.")
        result = method(*args, **kw)
        end_time = time.time()
        n_seconds = end_time - start_time
        if n_seconds < 60:
            logger.info(
                f"{method.__name__} ended successfully : {round(n_seconds, 1)}s to execute"
            )
        elif 60 < n_seconds < 3600:
            logger.info(
                f"{method.__name__} ended successfully : {round(n_seconds // 60)}min {round(n_seconds % 60, 1)}s to execute"
            )
        else:
            logger.info(
                f"{method.__name__} ended successfully : {round(n_seconds // 3600)}h {round(n_seconds % 3600 // 60)}min {round(n_seconds // 3600 % 60, 1)}s to execute"
            )
        return result

    return timed
