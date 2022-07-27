import utils
from flow import Flow


class RapSketch:
    def __init__(self, counters: int):
        self.size = counters
        self.count_map = {}

    def insert(self, flow: Flow):
        if flow.id in self.count_map:
            self.count_map[flow.id] += 1
            return
        elif len(self.count_map) < self.size:
            self.count_map[flow.id] = 1
            return
        else:
            min_elem = min(self.count_map, key=self.count_map.get)
            if utils.flip_coin(1 / (self.count_map[min_elem] + 1)):
                # replace minimal entry
                old_count = self.count_map[min_elem]
                del self.count_map[min_elem]
                self.count_map[flow.id] = old_count + 1

    def get_counts(self):
        return self.count_map

    def reset(self, hash_func_index):
        self.count_map = {}
