import utils
from flow import Flow
import random


class PrecisionSketch:
    class Entry:
        def __init__(self, id: str, count: int):
            self.id = id
            self.count = count

    data: [[Entry]]

    def __init__(self, counters: int, stages: int):
        self.stages = stages
        self.counters_per_stage = counters // stages
        self.data = [[None for _ in range(self.counters_per_stage)] for _ in range(stages)]
        self.hash = utils.generate_random_hash_function(random.randint(1, 256))

    def insert(self, flow: Flow):
        min_entry = None
        min_entry_stage = None
        for stage in range(self.stages):
            index = self.get_hash_index(flow.id, stage)
            if self.data[stage][index] is None:
                self.data[stage][index] = self.Entry(flow.id, 1)
                return
            elif self.data[stage][index].id == flow.id:
                self.data[stage][index].count += 1
                return
            if min_entry is None or min_entry.count > self.data[stage][index].count:
                min_entry = self.data[stage][index]
                min_entry_stage = stage
        # simulating re-circulation:
        # using 2-approximation for the recirculation probability to simulate hardware restrictions
        recirculation_probability = 1 / utils.next_power_of_2(int(min_entry.count + 1))
        if utils.flip_coin(recirculation_probability):
            index = self.get_hash_index(flow.id, min_entry_stage)
            self.data[min_entry_stage][index] = self.Entry(flow.id, 1/recirculation_probability)

    def get_hash_index(self, key: str, stage: int):
        return self.hash(key + ":stage:" + str(stage)) % self.counters_per_stage
        # concatenating stage# to simulate per-stage hash function

    def get_counts(self):
        counts = {}
        for i,stage in enumerate(range(self.stages)):
            for entry in self.data[stage]:
                if entry is None:
                    continue
                if entry.id in counts:
                    counts[entry.id] += entry.count
                    continue
                counts[entry.id] = entry.count
        return counts

    def reset(self, hash_func_index):
        self.data = [[None for _ in range(self.counters_per_stage)] for _ in range(self.stages)]
        if hash_func_index is None:
            hash_func_index = random.randint(1, 256)
        self.hash = utils.generate_random_hash_function(hash_func_index)
