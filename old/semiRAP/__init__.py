import random
from flow import Flow
import utils


class SemiRapSketch:
    class Entry:
        def __init__(self, id: str, count: int):
            self.id = id
            self.count = count

    data: [[Entry]]

    def __init__(self, counters: int, stages: int, approximated_probability=False, approximated_counter=False):
        self.stages = stages
        self.counters_per_stage = counters // stages
        self.data = [[None for _ in range(self.counters_per_stage)] for _ in range(stages)]
        self.hash = utils.generate_random_hash_function(random.randint(1, 256))
        self.statistics = utils.Statistics()
        self.counts = {}
        self.approximated_probability = approximated_probability
        self.approximated_counter = approximated_counter

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
        if self.data[0][index].id == flow.id:
            self.data[0][index].count += 1
            # update counts (helper structure)
            self.counts[flow.id] += 1
            return
        # using 2-approximation for the insertion probability to simulate hardware restrictions
        # elif utils.flip_coin(1 / utils.next_power_of_2(self.data[0][index].count + 1)):
        if self.approximated_probability:
            # using 2-approximation for the recirculation probability to simulate hardware restrictions
            insertion_probability = 1 / utils.next_power_of_2(self.data[0][index].count + 1)
        else:
            insertion_probability = 1 / (self.data[0][index].count + 1)
        if utils.flip_coin(insertion_probability):
            victim_entry = self.data[0][index]
            self.data[0][index] = self.Entry(flow.id, (victim_entry.count + 1) if not self.approximated_counter else utils.next_power_of_2(self.data[0][index].count + 1))
            # update counts helper structure
            if flow.id in self.counts:
                self.counts[flow.id] += victim_entry.count + 1
            else:
                self.counts[flow.id] = victim_entry.count + 1
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
            # update counts (helper structure) after replacement
            self.counts[victim_entry.id] -= victim_entry.count

        else:  # other case of coin flip. This is an optimization.
            for stage in range(1, self.stages):
                index = self.get_hash_index(flow.id, stage)
                if self.data[stage][index] is not None and self.data[stage][index].id == flow.id:
                    # update relevant entry if exists
                    self.data[stage][index].count += 1
                    # update counts (helper structure)
                    self.counts[flow.id] += 1
                    return

    def get_hash_index(self, key: str, stage: int):
        return self.hash(key + ":stage:" + str(stage)) % self.counters_per_stage
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