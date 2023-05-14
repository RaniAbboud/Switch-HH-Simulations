import math
import statistics

import utils
from hashpipe import HashPipeSketch
from cmsis import CMSIS
from precision import PrecisionSketch
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

if __name__ == '__main__':
    utils.theta = 2000
    number_of_trials = 1
    # trial_size = 2 * 10 ** 6
    trace_prefix_skip_stats_size = 1 * 10 ** 6

    memory_values_bytes = [16 * 1024 * (2 ** i) for i in range(7)]
    sketches_by_type = {
        'CMS': [CMSIS(memory_bytes=mem, entries_per_id_stage=0, id_stages=0, required_matches=0, insertion_p=0, theta=utils.theta) for mem in memory_values_bytes],
        'CMSIS-M1': [CMSIS(memory_bytes=mem, entries_per_id_stage=256, id_stages=3, required_matches=1, insertion_p=1/128, theta=utils.theta) for mem in memory_values_bytes],
        'CMSIS-M2': [CMSIS(memory_bytes=mem, entries_per_id_stage=256, id_stages=3, required_matches=2, insertion_p=1/128, theta=utils.theta) for mem in memory_values_bytes],
        '2W-PRECISION': [PrecisionSketch(memory_bytes=mem, stages=2, delay=20, theta=utils.theta) for mem in memory_values_bytes],
        '2W-HashPipe': [HashPipeSketch(memory_bytes=mem, stages=2, theta=utils.theta) for mem in memory_values_bytes],
        # '4W-HashPipe': [HashPipeSketch(memory_bytes=mem, stages=4) for mem in memory_values_bytes],
    }

    # Initializations
    recall_by_type = {sketch_type: [[] for i in range(len(memory_values_bytes))] for sketch_type in sketches_by_type.keys()}
    precision_by_type = {sketch_type: [[] for i in range(len(memory_values_bytes))] for sketch_type in sketches_by_type.keys()}
    mse_by_type = {sketch_type: [[] for i in range(len(memory_values_bytes))] for sketch_type in sketches_by_type.keys()}
    fpr_by_type = {sketch_type: [[] for i in range(len(memory_values_bytes))] for sketch_type in sketches_by_type.keys()}
    fnr_by_type = {sketch_type: [[] for i in range(len(memory_values_bytes))] for sketch_type in sketches_by_type.keys()}

    for trial in range(number_of_trials):
        print(f'Performing trial#{trial + 1}...')
        for trace_index in range(utils.get_num_of_trace_files()):
            print(f'Inserting trace#{trace_index}...')
            utils.load_data_file(trace_index)  # load the parsed data file (~22M 5-tuples) into memory
            all_sketches = [sketch for (_, sketches) in sketches_by_type.items() for sketch in sketches]
            # fill sketches
            utils.insert_data(sketches=all_sketches, stats_skip_count=trace_prefix_skip_stats_size)
            print('Done filling sketches.')
        # calculating Recall and MSE
        for sketch_type, sketches in sketches_by_type.items():
            for i, sketch in enumerate(sketches):
                stats = sketch.statistics
                # MSE
                mse = stats.mse/sketch.pkt_count
                mse_by_type[sketch_type][i].append(mse)
                # Recall
                recall = stats.tp/(stats.tp + stats.fn)
                recall_by_type[sketch_type][i].append(recall)
                # Precision
                precision = stats.tp/(stats.tp + stats.fp)
                precision_by_type[sketch_type][i].append(precision)
                # FPR
                fpr = stats.fp/(stats.fp + stats.tp)
                fpr_by_type[sketch_type][i].append(fpr)
                # FNR
                fnr = stats.fn/(stats.fn + stats.tn)
                fnr_by_type[sketch_type][i].append(fnr)
                print(f"{sketch_type} with {utils.kb_formatter(memory_values_bytes[i],None)} memory. Recall=", recall, f' MSE=10^{math.log10(mse)}')
        # clean sketches
        for sketch_type in sketches_by_type.keys():
            for sketch in sketches_by_type[sketch_type]:
                sketch.reset(hash_func_index=trial)
        utils.counter.clear()

    # Plotting
    # plotting average recall for each sketch
    plt.subplot(3, 2, 1)  # rows, cols, current number
    plt.xlabel('Memory')
    plt.xscale('log', base=2)
    plt.gca().xaxis.set_major_formatter(FuncFormatter(utils.kb_formatter))
    plt.ylabel('Recall')
    for sketch_type in sketches_by_type.keys():
        plt.plot(memory_values_bytes, [statistics.fmean(recall_list) for recall_list in recall_by_type[sketch_type]],
                 label=sketch_type)
    # plotting average Precision for each sketch
    plt.subplot(3, 2, 2)
    plt.xlabel('Memory')
    plt.xscale('log', base=2)
    plt.gca().xaxis.set_major_formatter(FuncFormatter(utils.kb_formatter))
    plt.ylabel('Precision')
    for sketch_type in sketches_by_type.keys():
        plt.plot(memory_values_bytes, [statistics.fmean(precision_list) for precision_list in precision_by_type[sketch_type]],
                 label=sketch_type)
    # plt.title(f'#trials={number_of_trials}, theta={utils.theta}', fontsize=10)
    # plotting average false-positives rate for each sketch
    plt.subplot(3, 2, 3)
    plt.xlabel('Memory')
    plt.xscale('log', base=2)
    plt.gca().xaxis.set_major_formatter(FuncFormatter(utils.kb_formatter))
    plt.ylabel('FPR')
    for sketch_type in sketches_by_type.keys():
        plt.plot(memory_values_bytes, [statistics.fmean(fp_list) for fp_list in fpr_by_type[sketch_type]],
                 label=sketch_type)
    # plotting average false-negative rate for each sketch
    plt.subplot(3, 2, 4)
    plt.xlabel('Memory')
    plt.xscale('log', base=2)
    plt.gca().xaxis.set_major_formatter(FuncFormatter(utils.kb_formatter))
    plt.ylabel('FNR')
    for sketch_type in sketches_by_type.keys():
        plt.plot(memory_values_bytes, [statistics.fmean(fn_list) for fn_list in fnr_by_type[sketch_type]],
                 label=sketch_type)
    # plotting average MSE for each sketch
    plt.subplot(3, 2, 5)
    plt.xlabel('Memory')
    plt.xscale('log', base=2)
    plt.gca().xaxis.set_major_formatter(FuncFormatter(utils.kb_formatter))
    plt.yscale('log', base=10)
    plt.ylabel('MSE')
    for sketch_type in sketches_by_type.keys():
        plt.plot(memory_values_bytes, [statistics.fmean(mse_list) for mse_list in mse_by_type[sketch_type]],
                 label=sketch_type)
    # plt.suptitle(f'Top-{k}, zipf-a={zipf_a}', fontsize=18, y=0.98)
    plt.suptitle(rf"CAIDA-{utils.caida_year}, $\theta$={1/utils.theta}", fontsize=18, y=0.98)
    plt.figlegend([sketch_name for sketch_name in sketches_by_type.keys()], loc="lower right")
    plt.tight_layout()
    plt.show(block=True)
