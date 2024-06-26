import utils
from flow import Flow
import random
from queue import Queue


class PrecisionSketch:
    class Entry:
        def __init__(self, id: str, count: int):
            self.id = id
            self.count = count

    class PendingUpdate:
        def __init__(self, new_flow_id, min_stage):
            self.new_flow_id = new_flow_id
            self.stage = min_stage

    def __init__(self, memory_bytes: int, stages=2, approximated_probability=False, delay=0, max_recirc_p=1/100, theta=1000):
        self.stages = stages
        self.counters_per_stage = (memory_bytes // 20) // stages  # each entry is 16B + 4B = 20B
        self.data = [[None for _ in range(self.counters_per_stage)] for _ in range(stages)]
        self.hash = utils.generate_random_hash_function(random.randint(1, 256))
        self.statistics = utils.Statistics()
        self.approximated_probability = approximated_probability
        self.max_recirc_p = max_recirc_p
        # self.approximated_counter = approximated_counter
        self.updates_delay = delay
        self.pending_updates = Queue(maxsize=0)  # infinite size
        self.pkt_count = 0
        self.theta = theta

    def insert(self, flow: Flow):
        self.pkt_count += 1
        # handling re-circulated packets
        if not self.pending_updates.empty():
            pending_update = self.pending_updates.get()  # pop update
            if pending_update is not None:  # if not a "delay" entry, replace old entry's id with re-circulated id
                index = self.get_hash_index(pending_update.new_flow_id, pending_update.stage)
                old_entry = self.data[pending_update.stage][index]
                self.data[pending_update.stage][index] = self.Entry(id=pending_update.new_flow_id,
                                                                    count=old_entry.count + 1)
        # ***** start insertion ***** #
        min_entry = None
        min_entry_stage = None
        for stage in range(self.stages):
            index = self.get_hash_index(flow.id, stage)
            if self.data[stage][index] is None:
                self.data[stage][index] = self.Entry(flow.id, 1)
                est = self.get_count(flow.id)
                return est, est >= self.pkt_count // self.theta
            elif self.data[stage][index].id == flow.id:
                self.data[stage][index].count += 1
                est = self.get_count(flow.id)
                return est, est >= self.pkt_count // self.theta
            if min_entry is None or min_entry.count > self.data[stage][index].count:
                min_entry = self.data[stage][index]
                min_entry_stage = stage
        # simulating re-circulation:
        if self.approximated_probability:
            # using 2-approximation for the recirculation probability to simulate hardware restrictions
            recirculation_probability = 1 / utils.next_power_of_2(int(min_entry.count + 1))
        else:
            recirculation_probability = 1 / (int(min_entry.count + 1))
        if utils.flip_coin(min(recirculation_probability, self.max_recirc_p)):
            # adding empty "pending updates" that will simulate the re-circulation delay
            for i in range(self.updates_delay):
                self.pending_updates.put(None)
            self.pending_updates.put(self.PendingUpdate(new_flow_id=flow.id, min_stage=min_entry_stage))
        return 1, 1 >= self.pkt_count // self.theta  # flow doesn't exist in data structure, feels like the first time we see it

    def get_hash_index(self, key: str, stage: int):
        return self.hash(key + ":stage:" + str(stage)) % self.counters_per_stage
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
        self.pending_updates = Queue(maxsize=0)
        self.pkt_count = 0

