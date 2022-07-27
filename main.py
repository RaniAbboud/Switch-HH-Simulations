# Data taken from https://www.kaggle.com/datasets/jsrojas/ip-network-traffic-flows-labeled-with-87-apps
import math

import utils
from hashpipe import HashPipeSketch
from optSemiRAP import OptimizedSemiRapSketch
from precision import PrecisionSketch
from rap import RapSketch
from semiRAP import SemiRapSketch
import matplotlib.pyplot as plt

if __name__ == '__main__':
    k = 32  # as in top-k
    # zipf_a = 2
    number_of_trials = 5

    # read kaggle data
    # df, true_top_k = utils.readKaggleData(k)

    counter_values = [k * (2 ** i) for i in range(7)]
    sketches_by_type = {
        '2W-SemiRAP (2-Apx)': [SemiRapSketch(counters=counters, stages=2) for counters in counter_values],
        # '4W-SemiRAP': [SemiRapSketch(counters=counters, stages=4) for counters in counter_values],
        # '6W-SemiRAP': [SemiRapSketch(counters=counters, stages=6) for counters in counter_values],
        # '2W-StableHashPipe': [OptimizedSemiRapSketch(counters=counters, stages=2) for counters in counter_values],
        # '4W-StableHashPipe': [OptimizedSemiRapSketch(counters=counters, stages=4) for counters in counter_values],
        # '6W-StableHashPipe': [OptimizedSemiRapSketch(counters=counters, stages=6) for counters in counter_values],
        # 'RAP': [RapSketch(counters=counters) for counters in counter_values],
        '2W-PRECISION (2-Apx)': [PrecisionSketch(counters=counters, stages=2) for counters in counter_values],
        # '4W-PRECISION': [PrecisionSketch(counters=counters, stages=4) for counters in counter_values],
        # '8W-PRECISION': [PrecisionSketch(counters=counters, stages=8) for counters in counter_values],
        '2W-HashPipe': [HashPipeSketch(counters=counters, stages=2) for counters in counter_values],
        '4W-HashPipe': [HashPipeSketch(counters=counters, stages=4) for counters in counter_values],
        '6W-HashPipe': [HashPipeSketch(counters=counters, stages=6) for counters in counter_values]
    }

    # Initializations
    recall_by_type = {sketch_type: [0] * len(counter_values) for sketch_type in sketches_by_type.keys()}
    mse_by_type = {sketch_type: [0] * len(counter_values) for sketch_type in sketches_by_type.keys()}

    for trial in range(number_of_trials):
        print(f'Performing trial#{trial + 1}...')
        # clean sketches
        for sketch_type in sketches_by_type.keys():
            for sketch in sketches_by_type[sketch_type]:
                sketch.reset(hash_func_index=trial)
        all_sketches = [sketch for (_, sketches) in sketches_by_type.items() for sketch in sketches]

        # generate zipf data
        # data, true_top_k = utils.generateZipfData(k, zipf_a, 3 * 10 ** 6)

        # read CAIDA data
        trial_size = 2*10**6
        # data, true_top_k = utils.readCaidaData(k, offset=trial*trial_size, n=trial_size)
        data, true_top_k, true_top_k_counts = utils.readCaidaData(k, offset=trial*trial_size, n=trial_size)

        # fill sketches
        utils.insert_zipf_data(all_sketches)
        # utils.insert_kaggle_data(all_sketches)
        print('Done filling sketches.')

        # calculating Recall and plotting
        for sketch_type, sketches in sketches_by_type.items():
            for i, sketch in enumerate(sketches):
                estimated_counts = sketch.get_counts()
                estimated_top_hitters = sketch.get_counts().keys()
                recall = sum([top_hitter in estimated_top_hitters for top_hitter in true_top_k]) / k
                recall_by_type[sketch_type][i] += recall
                mse = sum([(estimated_counts[flow]-flow_count)**2 if flow in estimated_counts
                           else flow_count**2
                           for (flow, flow_count) in true_top_k_counts])/k
                mse_by_type[sketch_type][i] += mse
                print(f"{sketch_type} with {counter_values[i]} counters. Recall=", recall, f' MSE=10^{math.log10(mse)}')

    # Plotting
    # plotting average recall for each sketch
    plt.subplot(1, 2, 1)
    plt.xlabel('Number of counters')
    plt.xscale('log', base=2)
    plt.ylabel('Recall')
    for sketch_type in sketches_by_type.keys():
        plt.plot(counter_values, [recall / number_of_trials for recall in recall_by_type[sketch_type]],
                 label=sketch_type)
    # plotting average MSE (for top-k flows) for each sketch
    plt.subplot(1, 2, 2)
    plt.xlabel('Number of counters')
    plt.xscale('log', base=2)
    plt.yscale('log', base=10)
    plt.ylabel('MSE')
    for sketch_type in sketches_by_type.keys():
        plt.plot(counter_values, [mse / number_of_trials for mse in mse_by_type[sketch_type]],
                 label=sketch_type)

    plt.title(f'#trials={number_of_trials}', fontsize=10)
    # plt.suptitle(f'Top-{k}, zipf-a={zipf_a}', fontsize=18, y=0.98)
    plt.suptitle(f'Top-{k}', fontsize=18, y=0.98)
    plt.legend()
    plt.tight_layout()
    plt.show()
