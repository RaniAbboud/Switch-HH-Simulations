from flow import Flow
import utils
import random


class OptimizedVotingSketch2:
    def __init__(self, counters: int, stages: int, first_stage_size=0.75, theta=1024, insertion_p=1/256):
        assert(stages > 2)
        self.stages = stages
        self.theta = theta
        self.counters_per_stage = {
            0: int(first_stage_size*counters/2),
            1: int(first_stage_size*counters/2)
        }
        for stage in range(2, stages):
            self.counters_per_stage[stage] = int((1-first_stage_size)*counters/(stages-2))

        self.data = [[None for _ in range(self.counters_per_stage[stage])] for stage in range(stages)]
        self.hash = utils.generate_random_hash_function(random.randint(1, 256))
        self.counts = {}
        self.statistics = utils.Statistics()
        self.pkt_count = 0
        self.insertion_p = insertion_p

    def insert(self, flow: Flow):
        self.pkt_count += 1
        for stage in range(2):
            if self.data[stage][self.get_hash_index(flow.id, stage)] is None:
                self.data[stage][self.get_hash_index(flow.id, stage)] = 1
                self.counts[flow.id] = 0
            else:
                self.data[stage][self.get_hash_index(flow.id, stage)] += 1
            # if (self.data[stage][self.get_hash_index(flow.id, stage)] or 0) < self.pkt_count // self.theta:
            #     self.counts[flow.id] = self.data[stage][self.get_hash_index(flow.id, stage)]
            #     return  # if first stage is < Threshold, return and don't update 2nd filter

        if (self.data[0][self.get_hash_index(flow.id, 0)] or 0) < self.pkt_count // self.theta or \
                (self.data[1][self.get_hash_index(flow.id, 1)] or 0) < self.pkt_count // self.theta:  # Threshold
            self.counts[flow.id] = self.get_count(flow.id)
            return

        if utils.flip_coin(self.insertion_p):
            # shift identifiers
            self.shift_row(flow.id, 2)
        self.counts[flow.id] = self.get_count(flow.id)  # for statistics

    def shift_row(self, flow_id, starting_stage=2):
        # insert flow_id in starting_stage, and shift
        new_val = flow_id
        for stage in range(starting_stage, self.stages):
            index = self.get_hash_index(new_val, stage)
            old_val = self.data[stage][index]
            self.data[stage][index] = new_val
            if old_val is None:
                break
            new_val = old_val

    def get_hash_index(self, key: str, stage: int):
        return hash(key + str(stage)) % self.counters_per_stage[stage]
        # concatenating stage# to simulate per-stage hash function

    def get_counts(self):
        top = {}
        for stage in range(2, self.stages):
            for flow_id in self.data[stage]:
                if flow_id in top or flow_id is None:
                    continue
                occurrences = 0
                for i in range(2, self.stages):
                    if self.data[i][self.get_hash_index(flow_id, i)] == flow_id:
                        occurrences += 1
                if occurrences < (self.stages-2)/2:  # require 2+ appearances
                    continue
                top[flow_id] = self.get_count(flow_id)
        return top

    def get_count(self, flow_id: str):
        est_count = min(self.data[0][self.get_hash_index(flow_id, 0)] or 0,
                        self.data[1][self.get_hash_index(flow_id, 1)] or 0)
        return est_count

    def reset(self, hash_func_index):
        self.data = [[None for _ in range(self.counters_per_stage[stage])] for stage in range(self.stages)]
        if hash_func_index is None:
            hash_func_index = random.randint(1, 256)
        self.hash = utils.generate_random_hash_function(hash_func_index)
        self.counts = {}
        self.statistics = utils.Statistics()
        self.pkt_count = 0
