import json
import os
from matplotlib.ticker import FuncFormatter
import statistics

import matplotlib.pyplot as plt
from os import listdir

stats_directory = "./statistics/offline/"

output_base_dir = "./offline_graphs/"


def kb_formatter(val, pos):
    return f'{int(val // 1024)}'


memory_values_bytes = [16 * 1024 * (2 ** i) for i in range(7)]


if __name__ == '__main__':
    for stat_file in sorted(listdir(stats_directory)):
        experiment_name = stat_file.split('.')[0]
        experiment_dir = output_base_dir + experiment_name + '/'
        if not os.path.exists(experiment_dir):
            os.makedirs(experiment_dir)

        with open(stats_directory + stat_file) as json_file:
            stats = json.load(json_file)
        sketch_names = sorted(list(stats['recall'].keys()))

        for stat in stats.keys():
            plt.close()
            plt.xlabel('Memory (KB)')
            plt.xscale('log', base=2)
            plt.gca().xaxis.set_major_formatter(FuncFormatter(kb_formatter))
            label = stat.capitalize()
            plt.ylabel(label)

            for sketch in sketch_names:
                colors = {
                    'CMSIS': 'C1',
                    'FCM+TopK': 'C3',
                    'PRECISION': 'C6',
                    'HashPipe': 'C5'
                }
                plt.plot(memory_values_bytes,
                         [statistics.fmean(recall_list) for recall_list in stats[stat][sketch]],
                         label=sketch, color=colors[sketch])

            ax = plt.gca()
            ax.set_ylim(bottom=0)
            plt.xticks(memory_values_bytes)
            plt.legend()

            fig = plt.gcf()
            fig.set_size_inches(4, 3)
            fig.savefig(f"{experiment_dir}{experiment_name}_{stat.capitalize()}.png", bbox_inches="tight")  #  , bbox_inches="tight")