from flow import Flow
import utils
import random


class HashPipeSketch:
    class Entry:
        def __init__(self, id: str, count: int):
            self.id = id
            self.count = count

    def __init__(self, memory_bytes: int, stages: int, theta=1000):
        self.stages = stages
        self.counters_per_stage = (memory_bytes // 20) // stages  # each entry is 16B + 4B = 20B
        self.data = [[None for _ in range(self.counters_per_stage)] for _ in range(stages)]
        self.hash = utils.generate_random_hash_function(random.randint(1, 256))
        self.statistics = utils.Statistics()
        self.pkt_count = 0
        self.theta = theta

    def insert(self, flow: Flow):
        self.pkt_count += 1
        index = self.get_hash_index(flow.id, 0)
        if self.data[0][index] is None:
            self.data[0][index] = self.Entry(flow.id, 1)
        elif self.data[0][index].id == flow.id:
            self.data[0][index].count += 1
        else:
            # insert flow with count=1 while replacing the existing entry
            victim_entry = self.data[0][index]
            self.data[0][index] = self.Entry(flow.id, 1)
            for stage in range(1, self.stages):
                index = self.get_hash_index(victim_entry.id, stage)
                if self.data[stage][index] is None:
                    self.data[stage][index] = victim_entry
                    break
                elif self.data[stage][index].id == victim_entry.id:  # merging duplicate entries
                    self.data[stage][index].count += victim_entry.count
                    break
                elif self.data[stage][index].count < victim_entry.count:
                    temp = self.data[stage][index]
                    self.data[stage][index] = victim_entry
                    victim_entry = temp
        est = self.get_count(flow.id)
        return est, est >= self.pkt_count // self.theta

    def get_hash_index(self, key: str, stage: int):
        return hash(key + ":stage:" + str(stage)) % self.counters_per_stage
        # concatenating stage# to simulate per-stage hash function

    def get_count(self, flow_id: str):
        est = 0
        for stage in range(self.stages):
            index = self.get_hash_index(flow_id, stage)
            if self.data[stage][index] is not None and self.data[stage][index].id == flow_id:
                est += self.data[stage][index].count
        return est

    def get_counts(self):
        counts = {}
        for stage in range(self.stages):
            for entry in self.data[stage]:
                if entry is None:
                    continue
                elif entry.id in counts:
                    counts[entry.id] += entry.count
                else:
                    counts[entry.id] = entry.count
        return counts

    def reset(self, hash_func_index):
        self.data = [[None for _ in range(self.counters_per_stage)] for _ in range(self.stages)]
        if hash_func_index is None:
            hash_func_index = random.randint(1, 256)
        self.hash = utils.generate_random_hash_function(hash_func_index)
        self.statistics.reset()
        self.pkt_count = 0


