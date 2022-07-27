class Flow:
    def __init__(self, flow_id: str):
        self.id = flow_id

    def __eq__(self, other):
        if isinstance(other, Flow):
            return self.id == other.id
        return False
