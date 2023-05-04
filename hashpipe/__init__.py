from flow import Flow
import utils
import random


class HashPipeSketch:
    class Entry:
        def __init__(self, id: str, count: int):
            self.id = id
            self.count = count

    data: [[Entry]]

    def __init__(self, memory_bytes: int, stages: int):
        self.stages = stages
        self.counters_per_stage = (memory_bytes // 20) // stages  # each entry is 16B + 4B = 20B
        self.data = [[None for _ in range(self.counters_per_stage)] for _ in range(stages)]
        self.hash = utils.generate_random_hash_function(random.randint(1, 256))
        self.statistics = utils.Statistics()
        self.counts = {}

    def insert(self, flow: Flow):
        index = self.get_hash_index(flow.id, 0)

        if self.data[0][index] is None:
            self.data[0][index] = self.Entry(flow.id, 1)
            # update counts (helper structure)
            if flow.id in self.counts:
                self.counts[flow.id] += 1
            else:
                self.counts[flow.id] = 1
            return
        elif self.data[0][index].id == flow.id:
            self.data[0][index].count += 1
            # update counts (helper structure)
            self.counts[flow.id] += 1
            return
        else:
            # insert flow with count=1 while replacing the existing entry
            victim_entry = self.data[0][index]
            self.data[0][index] = self.Entry(flow.id, 1)
            # update the counts (helper structure)
            if flow.id in self.counts:
                self.counts[flow.id] += 1
            else:
                self.counts[flow.id] = 1
            for stage in range(1, self.stages):
                index = self.get_hash_index(victim_entry.id, stage)
                if self.data[stage][index] is None:
                    self.data[stage][index] = victim_entry
                    return
                elif self.data[stage][index].id == victim_entry.id:  # merging duplicate entries
                    self.data[stage][index].count += victim_entry.count
                    return
                elif self.data[stage][index].count < victim_entry.count:
                    temp = self.data[stage][index]
                    self.data[stage][index] = victim_entry
                    victim_entry = temp
            # update the counts (helper structure) after replacement
            self.counts[victim_entry.id] -= victim_entry.count

    def get_hash_index(self, key: str, stage: int):
        return hash(key + ":stage:" + str(stage)) % self.counters_per_stage
        # concatenating stage# to simulate per-stage hash function

    def get_counts(self):
        return {key: val for key, val in self.counts.items() if val > 0}
        # counts = {}
        # for stage in range(self.stages):
        #     for entry in self.data[stage]:
        #         if entry is None:
        #             continue
        #         if entry.id in counts:
        #             counts[entry.id] += entry.count
        #             continue
        #         counts[entry.id] = entry.count
        # return counts

    def reset(self, hash_func_index):
        self.data = [[None for _ in range(self.counters_per_stage)] for _ in range(self.stages)]
        if hash_func_index is None:
            hash_func_index = random.randint(1, 256)
        self.hash = utils.generate_random_hash_function(hash_func_index)
        self.statistics = utils.Statistics()
        self.counts = {}

