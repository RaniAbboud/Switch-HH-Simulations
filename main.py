import math
import statistics

import utils
from hashpipe import HashPipeSketch
from cmsis import CMSIS
from precision import PrecisionSketch
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

if __name__ == '__main__':
    k = 32  # as in top-k
    # zipf_a = 2
    utils.theta = 1000
    number_of_trials = 3
    trial_size = 1 * 10 ** 6

    memory_values_bytes = [16 * 1024 * (2 ** i) for i in range(6)]
    sketches_by_type = {
        # 'CMSIS-M3': [CMSIS(memory_bytes=mem, entries_per_id_stage=128, id_stages=3, required_matches=3, insertion_p=1/128, theta=theta) for mem in memory_values_bytes],
        'CMSIS-M2': [CMSIS(memory_bytes=mem, entries_per_id_stage=256, id_stages=3, required_matches=2, insertion_p=1/64, theta=utils.theta) for mem in memory_values_bytes],
        '2W-PRECISION-20p': [PrecisionSketch(memory_bytes=mem, stages=2, delay=20, theta=utils.theta) for mem in memory_values_bytes],
        '2W-HashPipe': [HashPipeSketch(memory_bytes=mem, stages=2, theta=utils.theta) for mem in memory_values_bytes],
        # '4W-HashPipe': [HashPipeSketch(memory_bytes=mem, stages=4) for mem in memory_values_bytes],
    }

    # Initializations
    recall_by_type = {sketch_type: [[] for i in range(len(memory_values_bytes))] for sketch_type in sketches_by_type.keys()}
    mse_by_type = {sketch_type: [[] for i in range(len(memory_values_bytes))] for sketch_type in sketches_by_type.keys()}
    fpr_by_type = {sketch_type: [[] for i in range(len(memory_values_bytes))] for sketch_type in sketches_by_type.keys()}
    fnr_by_type = {sketch_type: [[] for i in range(len(memory_values_bytes))] for sketch_type in sketches_by_type.keys()}

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
        utils.readCaidaDataV2(k, offset=trial*trial_size, n=trial_size)

        # fill sketches
        utils.insert_data(all_sketches, calculate_statistics=True, stats_skip_count=100000)
        print('Done filling sketches.')

        # calculating Recall and MSE
        for sketch_type, sketches in sketches_by_type.items():
            for i, sketch in enumerate(sketches):
                stats = sketch.statistics
                # MSE
                mse = stats.mse/trial_size
                mse_by_type[sketch_type][i].append(mse)
                # Recall
                recall = stats.tp/(stats.tp + stats.fn)
                recall_by_type[sketch_type][i].append(recall)
                # FPR
                fpr = stats.fp/(stats.fp + stats.tp)
                fpr_by_type[sketch_type][i].append(fpr)
                # FNR
                fnr = stats.fn/(stats.fn + stats.tn)
                fnr_by_type[sketch_type][i].append(fnr)
                print(f"{sketch_type} with {utils.kb_formatter(memory_values_bytes[i],None)} memory. Recall=", recall, f' MSE=10^{math.log10(mse)}')

    # Plotting
    # plotting average recall for each sketch
    plt.subplot(2, 2, 1)  # rows, cols, current number
    plt.xlabel('Memory')
    plt.xscale('log', base=2)
    plt.gca().xaxis.set_major_formatter(FuncFormatter(utils.kb_formatter))
    plt.ylabel('Recall')
    for sketch_type in sketches_by_type.keys():
        plt.plot(memory_values_bytes, [statistics.fmean(recall_list) for recall_list in recall_by_type[sketch_type]],
                 label=sketch_type)
    # plotting average MSE (for top-k flows) for each sketch
    plt.subplot(2, 2, 2)
    plt.xlabel('Memory')
    plt.xscale('log', basex=2)
    plt.gca().xaxis.set_major_formatter(FuncFormatter(utils.kb_formatter))
    plt.yscale('log', basey=10)
    plt.ylabel('MSE')
    for sketch_type in sketches_by_type.keys():
        plt.plot(memory_values_bytes, [statistics.fmean(mse_list) for mse_list in mse_by_type[sketch_type]],
                 label=sketch_type)
    # plt.title(f'#trials={number_of_trials}, theta={utils.theta}', fontsize=10)
    # plotting average false-positives rate for each sketch
    plt.subplot(2, 2, 3)
    plt.xlabel('Memory')
    plt.xscale('log', basex=2)
    plt.ylabel('FPR')
    for sketch_type in sketches_by_type.keys():
        plt.plot(memory_values_bytes, [statistics.fmean(fp_list) for fp_list in fpr_by_type[sketch_type]],
                 label=sketch_type)
    # plotting average false-negative rate for each sketch
    plt.subplot(2, 2, 4)
    plt.xlabel('Memory')
    plt.xscale('log', basex=2)
    plt.ylabel('FNR')
    for sketch_type in sketches_by_type.keys():
        plt.plot(memory_values_bytes, [statistics.fmean(fn_list) for fn_list in fnr_by_type[sketch_type]],
                 label=sketch_type)

    # plt.suptitle(f'Top-{k}, zipf-a={zipf_a}', fontsize=18, y=0.98)
    plt.suptitle(f'theta={1/utils.theta}', fontsize=18, y=0.98)
    plt.legend()
    plt.tight_layout()
    plt.show(block=True)
