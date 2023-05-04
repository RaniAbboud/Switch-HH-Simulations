from flow import Flow
import utils
import random


class CMSIS:
    def __init__(self, memory_bytes: int, count_stages=2, id_stages=3, entries_per_id_stage=128, theta=1000, insertion_p=1/128):
        assert(memory_bytes > entries_per_id_stage * id_stages * 16)
        self.count_stages = count_stages
        self.id_stages = id_stages
        self.theta = theta

        self.entries_per_id_stage = entries_per_id_stage
        self.id_structure = [[None for _ in range(self.entries_per_id_stage)] for _ in range(id_stages)]

        memory_bytes_cms = memory_bytes - id_stages*entries_per_id_stage*16  # memory bytes left for CMS structure
        self.entries_per_count_stage = (memory_bytes_cms // 4) // count_stages  # each count entry is 4B
        self.count_structure = [[None for _ in range(self.entries_per_count_stage)] for _ in range(count_stages)]

        self.hash = utils.generate_random_hash_function(random.randint(1, 256))
        self.counts = {}
        self.statistics = utils.Statistics()
        self.pkt_count = 0
        self.insertion_p = insertion_p

    def insert(self, flow: Flow):
        self.pkt_count += 1
        estimations = []
        for stage in range(self.count_stages):
            index = self.get_count_hash_index(flow.id, stage)
            if self.count_structure[stage][index] is None:
                self.count_structure[stage][index] = 1
            else:
                self.count_structure[stage][index] += 1
            estimations.append(self.count_structure[stage][index])
        self.counts[flow.id] = self.get_count(flow.id)  # for statistics
        if min(estimations) < self.pkt_count // self.theta:  # Threshold
            return
        # We get here only if estimation > threshold
        if utils.flip_coin(self.insertion_p):
            # shift identifiers
            self.shift_row(flow.id)
        # TODO: online labelling

    def shift_row(self, flow_id):
        # insert flow_id and shift
        new_val = flow_id
        for stage in range(self.id_stages):
            index = self.get_id_hash_index(new_val, stage)
            old_val = self.id_structure[stage][index]
            self.id_structure[stage][index] = new_val
            if old_val is None:
                break
            new_val = old_val

    def get_id_hash_index(self, key: str, stage: int):
        return hash(key + str(stage) + 'id') % self.entries_per_id_stage
        # concatenating stage# to simulate per-stage hash function

    def get_count_hash_index(self, key: str, stage: int):
        return hash(key + str(stage) + 'count') % self.entries_per_count_stage
        # concatenating stage# to simulate per-stage hash function

    def get_counts(self):
        top = {}
        for stage in range(self.id_stages):
            for flow_id in self.id_structure[stage]:
                if flow_id in top or flow_id is None:
                    continue
                occurrences = 0
                for i in range(self.id_stages):
                    if self.id_structure[i][self.get_id_hash_index(flow_id, i)] == flow_id:
                        occurrences += 1
                if occurrences < 2:  # require 2+ appearances
                    continue
                top[flow_id] = self.get_count(flow_id)
        return top

    def get_count(self, flow_id: str):
        estimations = []
        for stage in range(self.count_stages):
            est = self.count_structure[stage][self.get_count_hash_index(flow_id, stage)]
            if est is None:
                return 0
            estimations.append(est)
        return min(estimations)

    def reset(self, hash_func_index):
        self.count_structure = [[None for _ in range(self.entries_per_count_stage)] for _ in range(self.count_stages)]
        self.id_structure = [[None for _ in range(self.entries_per_id_stage)] for _ in range(self.id_stages)]
        if hash_func_index is None:
            hash_func_index = random.randint(1, 256)
        self.hash = utils.generate_random_hash_function(hash_func_index)
        self.counts = {}
        self.statistics = utils.Statistics()
        self.pkt_count = 0
