import math
import random
# import pandas as pd
# from scipy.stats import zipf
from collections import Counter
from flow import Flow
from dataclasses import dataclass
from scapy.all import *


_memomask = {}

data = []

theta = 1000

preader = PcapReader('/Users/srani/Documents/roy/caida/equinix-chicago.dirA.20160121-125911.UTC.anon.pcap')


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


# def readCaidaData(k=32, offset=0, n=2 * 10 ** 6):
#     global data
#     with open('./data/caida_16_part1_parsed.txt') as file:
#         data = file.readlines()[offset:offset + n]
#     # print(Counter(data).most_common(128))
#     true_top_k = [flow_id for (flow_id, _) in Counter(data).most_common(k)]
#     true_top_k_counts = Counter(data).most_common(k)
#     return data, true_top_k, true_top_k_counts


def readCaidaDataV2(k=32, offset=0, n=2 * 10 ** 6):
    global data
    global preader
    for _ in range(n):
        p = next(preader)
        if p is None:
            print('PcapReader is empty.')
            break
        if IP not in p:
            continue
        flow_id = make_key(p)
        data.append(flow_id)
    # true_top_k = [flow_id for (flow_id, _) in Counter(data).most_common(k)]
    # true_top_k_counts = Counter(data).most_common(k)
    # return data, true_top_k, true_top_k_counts
    return data


def insert_data_to_sketch(sketch, calculate_statistics=True, stats_skip_count=100000):
    global data
    counter = Counter()
    for index, flow_id in enumerate(data):
        flow_id = str(flow_id)
        flow = Flow(flow_id)
        counter[flow_id] += 1
        # insert into sketch
        frequency_estimation, hh_label = sketch.insert(flow)
        if calculate_statistics and index >= stats_skip_count:
            # calculate estimation error
            sketch.statistics.mse += (counter[flow_id] - frequency_estimation) ** 2
            if counter[flow.id] >= (index + 1) // theta:  # Flow is HH
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


def insert_data(sketches, calculate_statistics=True, stats_skip_count=100000):
    threads = [threading.Thread(target=insert_data_to_sketch, args=(sketch, calculate_statistics, stats_skip_count))
               for sketch in sketches]
    # start threads
    for t in threads:
        t.start()
    # wait for all threads to finish
    for t in threads:
        t.join()


def kb_formatter(val, pos):
    return f'{int(val // 1024)}KB'


def make_key(packet):
    key = f'{packet[IP].src},{packet[IP].dst},{packet[IP].proto}'
    if TCP in packet:
        key = f'{key},{packet[TCP].sport},{packet[TCP].dport}'
    elif UDP in packet:
        key = f'{key},{packet[UDP].sport},{packet[UDP].dport}'
    return key