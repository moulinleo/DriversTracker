from functools import wraps
import time 

# Generator function to compute the execution time
def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        print(f"{func.__name__} - Time elapsed: {elapsed_time:.6f} seconds")
        return result
    return wrapper
