from flow import Flow
import utils
import random
from collections import Counter


class VotingSketch:
    def __init__(self, counters: int, stages: int, insertion_prob=0.0078125):
        assert(stages > 1)
        self.stages = stages
        self.counters_per_stage = counters // stages
        self.data = [[None for _ in range(self.counters_per_stage)] for _ in range(stages)]
        self.hash = utils.generate_random_hash_function(random.randint(1, 256))
        self.counts = {}
        self.insertion_prob = insertion_prob
        self.statistics = utils.Statistics()

    def insert(self, flow: Flow):
        index = self.get_hash_index(flow.id)
        if self.data[0][index] is None:
            self.data[0][index] = 1
        else:
            self.data[0][index] += 1
        insertion_p = 4/utils.next_power_of_2(self.data[0][index])
        if utils.flip_coin(insertion_p):
            # shift identifiers
            self.shift_row(flow.id)
        self.counts[flow.id] = self.get_count(flow.id)  # for statistics

    def shift_row(self, flow_id, starting_stage=1):
        # insert flow_id in starting_stage, and shift
        index = self.get_hash_index(flow_id)
        new_val = flow_id
        for stage in range(starting_stage, self.stages):
            old_val = self.data[stage][index]
            self.data[stage][index] = new_val
            new_val = old_val

    def get_hash_index(self, key: str, stage=0):
        return hash(key) % self.counters_per_stage
        # return hash(key + ":stage:" + str(stage)) % self.counters_per_stage
        # concatenating stage# to simulate per-stage hash function

    def get_counts(self):
        top = {}
        for row in range(self.counters_per_stage):
            nominees = [self.data[stage][row] for stage in range(1, self.stages) if self.data[stage][row] is not None]
            counts = Counter(nominees)
            for count in counts.items():
                if count[1]/len(nominees) >= 2/5:  # majority?
                    top[count[0]] = self.data[0][row]*(count[1]/len(nominees))
        return top

    def get_count(self, flow_id: str):
        index = self.get_hash_index(flow_id)
        nominees = [self.data[stage][index] for stage in range(1, self.stages) if self.data[stage][index] is not None]
        counts = Counter(nominees)
        for count in counts.items():
            if count[0] == flow_id:
                return self.data[0][index]*(count[1]/len(nominees))
        return 0

    def reset(self, hash_func_index):
        self.data = [[None for _ in range(self.counters_per_stage)] for _ in range(self.stages)]
        if hash_func_index is None:
            hash_func_index = random.randint(1, 256)
        self.hash = utils.generate_random_hash_function(hash_func_index)
        self.counts = {}
        self.statistics = utils.Statistics()


