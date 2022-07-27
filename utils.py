import random
import pandas as pd
from scipy.stats import zipf
from collections import Counter
from flow import Flow

_memomask = {}

data = {}

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
    return 1 if x == 0 else 2**(x - 1).bit_length()


def generateZipfData(k=32, a=1.1, n=10**6):
    global data
    data = zipf.rvs(a, size=n)
    true_top_k = [str(flow_id) for (flow_id, _) in Counter(data).most_common(k)]
    true_top_k_counts = Counter(data).most_common(k)
    return data, true_top_k, true_top_k_counts


def readKaggleData(k=128):
    global data
    df = pd.read_csv('./data/dataset.csv', usecols=['Source.IP', 'Destination.IP'])
    data = df
    print('Done reading Kaggle csv file.')
    true_hit_counts = df.groupby(['Source.IP', 'Destination.IP']).size().reset_index(name='count').sort_values(
        by='count',
        ascending=False)[:k]
    true_hit_counts = {row['Source.IP'] + ":" + row['Destination.IP']: row['count'] for _, row in
                       true_hit_counts.iterrows()}
    true_top_k = [flow_id for (flow_id, _) in true_hit_counts.items()]

    return df, true_top_k


def readCaidaData(k=32, offset=0, n=2*10**6):
    global data
    with open('./data/caida_16_part1_parsed.txt') as file:
        data = file.readlines()[offset:offset+n]
    true_top_k = [flow_id for (flow_id, _) in Counter(data).most_common(k)]
    true_top_k_counts = Counter(data).most_common(k)
    return data, true_top_k, true_top_k_counts


def insert_kaggle_data(sketches):
    for _, row in data.iterrows():
        flow = Flow(row['Source.IP'] + ":" + row['Destination.IP'])
        for sketch in sketches:
            sketch.insert(flow)


# insert_zipf_data: inserts the zipf data into the given sketches.
# Note that we could shuffle the data here, but instead we generate an entirely new zipf data for each trial
# that we perform.
def insert_zipf_data(sketches):
    for flow_id in data:
        flow = Flow(str(flow_id))
        for sketch in sketches:
            sketch.insert(flow)
