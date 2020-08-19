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
        self = plasma.connect(path, num_retries=5)

    # def get(self, x):
    #     return self.client.get(x)

    # def put(self, x, y):
    #     return self.client.put(x,y)

    # def list(self):
    #     return self.client.list()

    # def delete(self,x):
    #     return self.client.delete(x)
