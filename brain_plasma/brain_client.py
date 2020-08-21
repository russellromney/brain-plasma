from functools import wraps
from pyarrow import plasma


def print_call(f):
    @wraps(f)
    def newfunc(*args, **kwargs):
        print("client call")
        return f(*args, **kwargs)

    return newfunc


class BrainClient:
    def __init__(self, path):
        # self.client = plasma.connect(path, num_retries=5)
        self.client = plasma.connect(path, num_retries=5)

    def put(self, *args, **kwargs):
        return self.client.put(*args, **kwargs)

    def get(self, *args, **kwargs):
        return self.client.get(*args, **kwargs)

    def list(self):
        return self.client.list()

    def store_capacity(self):
        return self.client.store_capacity()

    def delete(self, *args, **kwargs):
        return self.client.delete(*args, **kwargs)
    
    def contains(self, *args, **kwargs):
        return self.client.contains(*args, **kwargs)

    def disconnect(self):
        return self.client.disconnect()