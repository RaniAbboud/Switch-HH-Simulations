from flow import Flow
import utils
import random


class CMSIS:
    def __init__(self, memory_bytes: int, count_stages=2, id_stages=3, entries_per_id_stage=128, theta=1000, insertion_p=1/128, required_matches=2):
        assert(memory_bytes > entries_per_id_stage * id_stages * 16)
        self.count_stages = count_stages
        self.id_stages = id_stages
        self.theta = theta

        self.entries_per_id_stage = entries_per_id_stage
        self.id_structure = [[None for _ in range(self.entries_per_id_stage)] for _ in range(id_stages)]

        memory_bytes_cms = memory_bytes - id_stages*entries_per_id_stage*16  # memory bytes left for CMS structure
        self.entries_per_count_stage = (memory_bytes_cms // 4) // count_stages  # each count entry is 4B
        self.count_structure = [[0 for _ in range(self.entries_per_count_stage)] for _ in range(count_stages)]

        self.hash = utils.generate_random_hash_function(random.randint(1, 256))
        self.statistics = utils.Statistics()
        self.pkt_count = 0
        self.insertion_p = insertion_p
        self.required_matches = required_matches

    def insert(self, flow: Flow):
        self.pkt_count += 1
        # CMS Update
        estimations = []
        for stage in range(self.count_stages):
            index = self.get_count_hash_index(flow.id, stage)
            self.count_structure[stage][index] += 1
            estimations.append(self.count_structure[stage][index])
        estimation = min(estimations)
        threshold = self.pkt_count // self.theta
        if estimation >= threshold and utils.flip_coin(self.insertion_p):
            # shift identifiers in ID structure
            self.shift_row(flow.id)
            return estimation, True
        elif estimation >= threshold:
            return estimation, self.count_id_matches(flow.id) >= self.required_matches
        return estimation, False

    def count_id_matches(self, flow_id: str):
        occurrences = 0
        for stage in range(self.id_stages):
            if self.id_structure[stage][self.get_id_hash_index(flow_id, stage)] == flow_id:
                occurrences += 1
        return occurrences

    def shift_row(self, flow_id):
        # insert flow_id and shift
        new_val = flow_id
        for stage in range(self.id_stages):
            index = self.get_id_hash_index(new_val, stage)
            old_val = self.id_structure[stage][index]
            self.id_structure[stage][index] = new_val
            if old_val is None:
                return
            new_val = old_val

    def get_id_hash_index(self, key: str, stage: int):
        return self.hash(key + 'id' + str(stage)) % self.entries_per_id_stage
        # concatenating stage# to simulate per-stage hash function

    def get_count_hash_index(self, key: str, stage: int):
        return self.hash(key + 'count' + str(stage)) % self.entries_per_count_stage
        # concatenating stage# to simulate per-stage hash function

    def get_counts(self):
        top = {}
        for stage in range(self.id_stages):
            for flow_id in self.id_structure[stage]:
                if flow_id in top or flow_id is None:
                    continue
                estimation = self.get_count(flow_id)
                top[flow_id] = estimation
        return top

    def get_count(self, flow_id: str):
        estimations = []
        for stage in range(self.count_stages):
            est = self.count_structure[stage][self.get_count_hash_index(flow_id, stage)]
            estimations.append(est)
        return min(estimations)

    def reset(self, hash_func_index):
        self.count_structure = [[0 for _ in range(self.entries_per_count_stage)] for _ in range(self.count_stages)]
        self.id_structure = [[None for _ in range(self.entries_per_id_stage)] for _ in range(self.id_stages)]
        if hash_func_index is None:
            hash_func_index = random.randint(1, 256)
        self.hash = utils.generate_random_hash_function(hash_func_index)
        self.statistics.reset()
        self.pkt_count = 0
