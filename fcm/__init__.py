from flow import Flow
import utils
import random


class FCMSketch:
    def __init__(self, memory_bytes: int, n_trees=2, stages=3, k=8, theta=1000):
        self.base_reg_width = 8  # width base_reg_width*(2**i) for stage i
        self.n_trees = n_trees
        first_stage_counters = (memory_bytes//n_trees)//sum([((2/k)**i) for i in range(stages)])
        self.entries_per_stage = {
            i: int(first_stage_counters*((1/k)**i)) for i in range(stages)
        }
        self.stages = stages
        self.data = [[[0 for _ in range(self.entries_per_stage[stage])] for stage in range(self.stages)]
                     for _ in range(self.n_trees)]
        self.theta = theta
        self.hash = utils.generate_random_hash_function(random.randint(1, 256))
        self.statistics = utils.Statistics()
        self.pkt_count = 0

    def insert(self, flow: Flow):
        self.pkt_count += 1
        # CMS Update
        estimations = []
        for tree in range(self.n_trees):
            counters = []
            for stage in range(self.stages):
                overflow_val = 2**(self.base_reg_width*(2**stage)) - 1
                index = self.get_hash_index(flow.id, stage, tree)
                if self.data[tree][stage][index] >= overflow_val:
                    counters.append(self.data[tree][stage][index])
                    continue
                self.data[tree][stage][index] += 1
                counters.append(self.data[tree][stage][index])
                break  # did not reach overflow, not continuing to next stage
            estimations.append(sum(counters))
        estimation = min(estimations)
        threshold = self.pkt_count // self.theta
        return estimation, estimation >= threshold

    def get_hash_index(self, key: str, stage: int, tree: int):
        return self.hash(key + str(stage) + '@' + str(tree)) % self.entries_per_stage[stage]
        # concatenating stage# to simulate per-stage hash function

    # def get_counts(self):
    #   pass

    def get_count(self, flow_id: str):
        estimations = []
        for tree in range(self.n_trees):
            counters = []
            for stage in range(self.stages):
                overflow_val = self.base_reg_width*(2**stage) - 1
                index = self.get_hash_index(flow_id, stage, tree)
                if self.data[tree][stage][index] >= overflow_val:
                    counters.append(self.data[tree][stage][index])
                    continue
                counters.append(self.data[tree][stage][index])
            estimations.append(sum(counters))
        return min(estimations)

    def reset(self, hash_func_index):
        self.data = [[[0 for _ in range(self.entries_per_stage[stage])] for stage in range(self.stages)]
                     for _ in range(self.n_trees)]
        if hash_func_index is None:
            hash_func_index = random.randint(1, 256)
        self.hash = utils.generate_random_hash_function(hash_func_index)
        self.statistics.reset()
        self.pkt_count = 0
