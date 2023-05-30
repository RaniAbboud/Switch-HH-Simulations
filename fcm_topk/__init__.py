from flow import Flow
import utils
import random
import fcm

HEAVY_ROW_SIZE_BYTES = 24  # each row has 4B all counter + 4B heavy-flow counter + 16B ID = 24B


class FCMTopK:
    class HeavyRow:
        def __init__(self):
            self.all_counter = 0
            self.heavy_counter = 0
            self.heavy_id = None

    def __init__(self, memory_bytes: int, heavy_lambda=8, fcm_n_trees=2, fcm_stages=3, fcm_k=8, theta=1000):
        self.heavy_structure_size = min(memory_bytes//(2*HEAVY_ROW_SIZE_BYTES), 2**12)
        heavy_mem = self.heavy_structure_size * HEAVY_ROW_SIZE_BYTES
        self.fcm = fcm.FCMSketch(memory_bytes=memory_bytes-heavy_mem, n_trees=fcm_n_trees, stages=fcm_stages, k=fcm_k)
        self.heavy_structure = [FCMTopK.HeavyRow() for _ in range(self.heavy_structure_size)]
        self.heavy_lambda = heavy_lambda

        self.theta = theta
        self.hash = utils.generate_random_hash_function(random.randint(1, 256))
        self.statistics = utils.Statistics()
        self.pkt_count = 0

    def insert(self, flow: Flow):
        self.pkt_count += 1
        threshold = self.pkt_count // self.theta

        index = self.get_hash_index(flow.id)
        self.heavy_structure[index].all_counter += 1
        if self.heavy_structure[index].heavy_id is None:
            self.heavy_structure[index].heavy_id = flow.id
            self.heavy_structure[index].heavy_counter = 1
            estimation = 1
        elif self.heavy_structure[index].heavy_id == flow.id:
            self.heavy_structure[index].heavy_counter += 1
            estimation = self.heavy_structure[index].heavy_counter
        else:  # different ID
            if self.heavy_structure[index].all_counter // self.heavy_lambda >= self.heavy_structure[index].heavy_counter:
                # insert prev heavy entry to FCM
                self.fcm.insert(Flow(self.heavy_structure[index].heavy_id), num_insertions=self.heavy_structure[index].heavy_counter)
                # swap entry in heavy structure
                self.heavy_structure[index].heavy_id = flow.id
                self.heavy_structure[index].heavy_counter += 1
                estimation = self.heavy_structure[index].heavy_counter
            else:
                estimation, _ = self.fcm.insert(flow)
        return estimation, estimation >= threshold

    def get_hash_index(self, key: str):
        return self.hash(key) % self.heavy_structure_size

    def get_counts(self):
        top = {}
        for heavy_row in self.heavy_structure:
            top[heavy_row.heavy_id] = heavy_row.heavy_counter

    def get_count(self, flow_id: str):
        # try heavy structure first
        index = self.get_hash_index(flow_id)
        if self.heavy_structure[index].heavy_id == flow_id:
            return self.heavy_structure[index].heavy_counter
        # if not found in heavy structure, return count estimate from FCM
        return self.fcm.get_count(flow_id)

    def reset(self, hash_func_index):
        self.fcm.reset(hash_func_index=hash_func_index)
        self.heavy_structure = [FCMTopK.HeavyRow() for _ in range(self.heavy_structure_size)]
        if hash_func_index is None:
            hash_func_index = random.randint(1, 256)
        self.hash = utils.generate_random_hash_function(hash_func_index)
        self.statistics.reset()
        self.pkt_count = 0
