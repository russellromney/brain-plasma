from pyarrow import plasma


class MockPlasmaClient:
    """
    uses a dictionary to emulate an actual plasma_store client
    """

    def __init__(self, path):
        self.path = path
        self.data = {}

    def get(self, value_id, *args, **kwargs):
        if isinstance(value_id, list):
            return [self.data[x] for x in value_id]
        return self.data[value_id]

    def put(self, thing, value_id):
        self.data[value_id] = thing

    def list(self):
        return {key: {"data_size": val.__sizeof__()} for key, val in self.data.items()}

    def delete(self, value_id):
        self.data.pop(value_id[0])

    def store_capacity(self):
        return 10000

    def contains(self, value_id):
        return value_id in self.data
