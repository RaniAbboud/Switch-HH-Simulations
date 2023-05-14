from collections import Counter
from flow import Flow
from dataclasses import dataclass
from scapy.all import *
from os import listdir

_memomask = {}

theta = 1000

caida_year = 18

parsed_traces_directory = f'./parsed_traces/caida_{caida_year}/'

data = []
counter = Counter()


@dataclass
class Statistics:
    def __init__(self):
        self.mse = float(0)
        self.fn = 0
        self.tp = 0
        self.fp = 0
        self.tn = 0

    def reset(self):
        self.mse = float(0)
        self.fn = 0
        self.tp = 0
        self.fp = 0
        self.tn = 0


def generate_random_hash_function(n):
    suffix = _memomask.get(n)
    if suffix is None:
        random.seed(n)
        suffix = _memomask[n] = random.getrandbits(64)

    def myhash(x):
        return hash(x + str(suffix))

    return myhash


def flip_coin(p):
    return True if random.random() < p else False


def next_power_of_2(x):
    return 1 if x == 0 else 2 ** ((x - 1).bit_length())


def get_num_of_trace_files():
    return len(listdir(parsed_traces_directory))


def load_data_file(file_index=0):
    global data
    global counter
    if data != []:
        counter += Counter(data)  # if this is not the first data file, save counts into the global counter
    parsed_trace_file_path = sorted(listdir(parsed_traces_directory))[file_index]
    with open(parsed_traces_directory + parsed_trace_file_path) as file:
        data = [line.rstrip() for line in file]


def insert_data_to_sketch(sketch, stats_skip_count):
    global counter
    global counter_total
    local_counter = Counter()
    for index, flow_id in enumerate(data):
        flow_id = str(flow_id)
        flow = Flow(flow_id)
        local_counter[flow_id] += 1
        # insert into sketch
        frequency_estimation, hh_label = sketch.insert(flow)
        if counter or index >= stats_skip_count:  # if this is not the first data file OR already inserted 1M
            real_count = counter[flow.id] + local_counter[flow.id]
            # calculate estimation error
            sketch.statistics.mse += (real_count - frequency_estimation) ** 2
            if real_count >= sketch.pkt_count // theta:  # Flow is HH
                if hh_label:
                    # correctly identified as HH
                    sketch.statistics.tp += 1
                else:
                    # missed HH
                    sketch.statistics.fn += 1
            else:  # Flow is NOT a HH
                if hh_label:
                    # wrongly identified as HH
                    sketch.statistics.fp += 1
                else:
                    # correctly identified as NON HH
                    sketch.statistics.tn += 1


def insert_data(sketches, stats_skip_count):
    threads = [threading.Thread(target=insert_data_to_sketch, args=(sketch, stats_skip_count))
               for sketch in sketches]
    # start threads
    for t in threads:
        t.start()
    # wait for all threads to finish
    for t in threads:
        t.join()


def kb_formatter(val, pos):
    return f'{int(val // 1024)}KB'
