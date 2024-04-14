import time
from functools import wraps
import logging


def func_timer_async(func):
    """
    create a timing decorator async function
    """
    @wraps(func)  # improves debugging
    async def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = await func(*args, **kwargs)
        end = time.perf_counter()
        logging.info(f'{func.__name__} took {(end - start) * 1000:.2f} ms')
        return result
    return wrapper


def func_timer(func):
    """
    create a timing decorator function
    """
    @wraps(func)  # improves debugging
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        # first_arg = args[1] if len(args) > 1 else 'N/A'
        # logging.info(f'{func.__name__}({first_arg}, {kwargs}) took {(end - start) * 1000:.4f} ms')
        logging.info(f'{func.__name__} took {(end - start) * 1000:.4f} ms')
        return result
    return wrapper
