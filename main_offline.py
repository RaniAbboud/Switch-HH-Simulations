import json
import math
import statistics
from collections import Counter

import utils
from fcm_topk import FCMTopK
from hashpipe import HashPipeSketch
from cmsis import CMSIS
from precision import PrecisionSketch
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

stats_directory = "./statistics/"

if __name__ == '__main__':
    utils.theta = 1000
    number_of_trials = 5
    trial_size = 10 * 10 ** 6

    memory_values_bytes = [16 * 1024 * (2 ** i) for i in range(7)]
    sketches_by_type = {
        'CMSIS': [CMSIS(memory_bytes=mem, entries_per_id_stage=128, id_stages=3, required_matches=1, insertion_p=1/128, theta=utils.theta) for mem in memory_values_bytes],
        'PRECISION': [PrecisionSketch(memory_bytes=mem, stages=2, delay=20, theta=utils.theta) for mem in memory_values_bytes],
        'HashPipe': [HashPipeSketch(memory_bytes=mem, stages=2, theta=utils.theta) for mem in memory_values_bytes],
        'FCM+TopK': [FCMTopK(memory_bytes=mem, theta=utils.theta) for mem in memory_values_bytes]
    }

    # Initializations
    recall_by_type = {sketch_type: [[] for i in range(len(memory_values_bytes))] for sketch_type in sketches_by_type.keys()}

    for trial in range(number_of_trials):
        print(f'Performing trial#{trial + 1}...')

        # load 20M from each file, 10M in each trial
        utils.load_data_file(trial//2)
        utils.data = utils.data[:trial_size]
        if trial % 2 != 0:
            utils.data = utils.data[trial_size:trial_size*2]

        all_sketches = [sketch for (_, sketches) in sketches_by_type.items() for sketch in sketches]
        # fill sketches
        utils.insert_data(sketches=all_sketches, calculate_online_stats=False)
        print('Done filling sketches.')
        # calculating Recall and MSE
        counter = Counter(utils.data)
        top_flows = {flow: count for flow, count in counter.items() if count >= trial_size//utils.theta}
        for sketch_type, sketches in sketches_by_type.items():
            for i, sketch in enumerate(sketches):
                # Offline Recall
                estimated_top_flows = sketch.get_counts().keys()
                recall = len([flow for flow in estimated_top_flows if flow in top_flows])/len(top_flows)
                recall_by_type[sketch_type][i].append(recall)
                print(f"{sketch_type} with {utils.kb_formatter(memory_values_bytes[i],None)} memory. Recall=", recall)
        # clean sketches
        for sketch_type in sketches_by_type.keys():
            for sketch in sketches_by_type[sketch_type]:
                sketch.reset(hash_func_index=trial)
        utils.counter.clear()

    with open(f"{stats_directory}caida{utils.caida_year}_th{utils.theta//1000}k_offline.json", "w") as stats_file:
        stats = {
            "recall": recall_by_type
        }
        json.dump(stats, stats_file, indent=4)

    # Plotting
    # plotting average recall for each sketch
    plt.xlabel('Memory')
    plt.xscale('log', base=2)
    plt.gca().xaxis.set_major_formatter(FuncFormatter(utils.kb_formatter))
    plt.ylabel('Recall')
    for sketch_type in sketches_by_type.keys():
        plt.plot(memory_values_bytes, [statistics.fmean(recall_list) for recall_list in recall_by_type[sketch_type]],
                 label=sketch_type)
    plt.suptitle(rf"CAIDA-{utils.caida_year}, $\theta$={1/utils.theta}", fontsize=18, y=0.98)
    plt.figlegend([sketch_name for sketch_name in sketches_by_type.keys()], loc="lower right")
    plt.tight_layout()
    plt.savefig(f'caida{utils.caida_year}_th{utils.theta//1000}k_offline.png')
    # plt.show(block=True)
