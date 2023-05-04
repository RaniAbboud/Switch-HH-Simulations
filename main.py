import math
import statistics

import utils
from hashpipe import HashPipeSketch
from cmsis import CMSIS
from precision import PrecisionSketch
import matplotlib.pyplot as plt

if __name__ == '__main__':
    k = 32  # as in top-k
    # zipf_a = 2
    theta = 1000
    number_of_trials = 10

    memory_values_bytes = [32 * 1024 * (2 ** i) for i in range(6)]
    sketches_by_type = {
        'CMSIS-M2': [CMSIS(memory_bytes=mem, insertion_p=1/128, theta=theta) for mem in memory_values_bytes],
        '2W-PRECISION': [PrecisionSketch(memory_bytes=mem, stages=2, approximated_probability=False, delay=50) for mem in memory_values_bytes],
        '2W-HashPipe': [HashPipeSketch(memory_bytes=mem, stages=2) for mem in memory_values_bytes],
        '4W-HashPipe': [HashPipeSketch(memory_bytes=mem, stages=4) for mem in memory_values_bytes],
    }

    # Initializations
    recall_by_type = {sketch_type: [[] for i in range(len(memory_values_bytes))] for sketch_type in sketches_by_type.keys()}
    mse_by_type = {sketch_type: [[] for i in range(len(memory_values_bytes))] for sketch_type in sketches_by_type.keys()}
    # false_positives_by_type = {sketch_type: [[] for i in range(len(memory_values_bytes))] for sketch_type in sketches_by_type.keys()}

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
        utils.insert_data(all_sketches, calculate_mse=True)
        # utils.insert_kaggle_data(all_sketches)
        print('Done filling sketches.')

        # calculating Recall and plotting
        for sketch_type, sketches in sketches_by_type.items():
            for i, sketch in enumerate(sketches):
                estimated_counts = sketch.get_counts()
                estimated_top_hitters = sketch.get_counts().keys()
                recall = sum([top_hitter in estimated_top_hitters for top_hitter in true_top_k]) / k
                recall_by_type[sketch_type][i].append(recall)
                # false_positives = sum([estimated_hitter not in true_top_k for estimated_hitter in estimated_top_hitters]) / len(estimated_top_hitters)
                # false_positives_by_type[sketch_type][i].append(false_positives)
                # mse = sum([(estimated_counts[flow]-flow_count)**2 if flow in estimated_counts
                #            else flow_count**2
                #            for (flow, flow_count) in true_top_k_counts])/k
                # mse_by_type[sketch_type][i] += mse
                mse = sketch.statistics.mse/trial_size
                mse_by_type[sketch_type][i].append(mse)
                print(f"{sketch_type} with {memory_values_bytes[i]} memory (bytes). Recall=", recall, f' MSE=10^{math.log10(mse)}')

    # Plotting
    # plotting average recall for each sketch
    plt.subplot(1, 2, 1)  # rows, cols, current number
    plt.xlabel('Number of counters')
    plt.xscale('log', base=2)
    plt.ylabel('Recall')
    for sketch_type in sketches_by_type.keys():
        plt.plot(memory_values_bytes, [statistics.fmean(recall_list) for recall_list in recall_by_type[sketch_type]],
                 label=sketch_type)
    # plotting average MSE (for top-k flows) for each sketch
    plt.subplot(1, 2, 2)
    plt.xlabel('Memory (bytes)')
    plt.xscale('log', base=2)
    plt.yscale('log', base=10)
    plt.ylabel('MSE')
    for sketch_type in sketches_by_type.keys():
        plt.plot(memory_values_bytes, [statistics.fmean(mse_list) for mse_list in mse_by_type[sketch_type]],
                 label=sketch_type)
    plt.title(f'#trials={number_of_trials}', fontsize=10)
    # # plotting average false-positives rate for each sketch
    # plt.subplot(2, 2, 3)
    # plt.xlabel('Number of counters')
    # plt.xscale('log', base=2)
    # plt.ylabel('False Positives')
    # for sketch_type in sketches_by_type.keys():
    #     plt.plot(counter_values, [statistics.fmean(fp_list) for fp_list in false_positives_by_type[sketch_type]],
    #              label=sketch_type)

    # plt.suptitle(f'Top-{k}, zipf-a={zipf_a}', fontsize=18, y=0.98)
    plt.suptitle(f'Top-{k}', fontsize=18, y=0.98)
    plt.legend()
    plt.tight_layout()
    plt.show(block=True)
