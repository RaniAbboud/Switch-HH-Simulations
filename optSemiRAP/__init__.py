import random
from flow import Flow
import utils


class OptimizedSemiRapSketch:
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
        for stage in range(self.stages):
            index = self.get_hash_index(flow.id, stage)
            existing_entry = self.data[stage][index]
            if existing_entry is None:
                self.data[stage][index] = self.Entry(flow.id, 1)
                return
            if existing_entry.id == flow.id:
                self.data[stage][index].count += 1
                return
            if stage == 0:  # insert only in first stage
                # insertion_prob = max(1 / (existing_entry.count + 1), 1 / 2**10)
                # insertion_prob = 1 / (existing_entry.count + 1)
                # if self.counters_per_stage*self.stages > 128*self.stages:
                #     insertion_prob = max(1 / (existing_entry.count + 1), 1 / 2**7)
                insertion_prob = 1 / 64
                if utils.flip_coin(insertion_prob):
                    victim_entry = existing_entry
                    self.data[stage][index] = self.Entry(flow.id, 1/insertion_prob)
                    for later_stage in range(1, self.stages):
                        index = self.get_hash_index(victim_entry.id, later_stage)
                        if self.data[later_stage][index] is None:
                            self.data[later_stage][index] = victim_entry
                            return
                        elif self.data[later_stage][index].id == victim_entry.id:  # merging duplicate entries
                            self.data[later_stage][index].count += victim_entry.count
                            return
                        elif self.data[later_stage][index].count < victim_entry.count:
                            temp = self.data[later_stage][index]
                            self.data[later_stage][index] = victim_entry
                            victim_entry = temp
                    return

    def get_hash_index(self, key: str, stage: int):
        return self.hash(key + ":stage:" + str(stage)) % self.counters_per_stage
        # concatenating stage# to simulate per-stage hash function

    def get_counts(self):
        counts = {}
        for stage in range(self.stages):
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

