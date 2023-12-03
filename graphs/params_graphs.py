import json
import os
import re

import matplotlib
from matplotlib.ticker import FuncFormatter, PercentFormatter
from matplotlib.pyplot import figure
import statistics

import matplotlib.pyplot as plt
from os import listdir

stats_directory = "./statistics/params/"

output_base_dir = "./params_graphs/"


def atoi(text):
    return int(text) if text.isdigit() else text


def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
    return [atoi(c) for c in re.split(r'(\d+)', text)]


def kb_formatter(val, pos):
    return f'{int(val // 1024)}'


if __name__ == '__main__':
    for stat_file in sorted(listdir(stats_directory)):
        if 'th2k' in stat_file:
            memory_values_bytes = [32 * 1024 * (2 ** i) for i in range(7)]
        else:
            memory_values_bytes = [16 * 1024 * (2 ** i) for i in range(7)]

        experiment_name = stat_file.split('.')[0]
        experiment_dir = output_base_dir + experiment_name + '/'
        if not os.path.exists(experiment_dir):
            os.makedirs(experiment_dir)

        with open(stats_directory + stat_file) as json_file:
            stats = json.load(json_file)
        sketch_names = list(stats['recall'].keys())
        sketch_names.sort(key=natural_keys)
        sketch_pretty_names = {name: name for name in sketch_names}
        sketch_pretty_names['CMS'] = 'CMS+Th'

        f1 = {
            sketch: [2*statistics.mean(stats['precision'][sketch][i])*statistics.mean(stats['recall'][sketch][i])/(statistics.fmean(stats['precision'][sketch][i])+statistics.mean(stats['recall'][sketch][i])) for i in range(len(memory_values_bytes))] for sketch in sketch_names
        }
        plt.close()
        plt.cla()
        plt.clf()
        plt.xlabel('Memory (KB)')
        plt.xscale('log', base=2)
        plt.gca().xaxis.set_major_formatter(FuncFormatter(kb_formatter))
        plt.ylabel('F1-score')
        for sketch in sketch_names:
            color = 'C1' if 'M1' in sketch else 'C2'
            marker = 'o'
            if '64' in sketch:
                marker = '+'
            if '128' in sketch:
                marker = 's'
            if '256' in sketch:
                marker = 'x'
            plt.plot(memory_values_bytes,
                     f1[sketch],
                     label=sketch_pretty_names[sketch])
        plt.xticks(memory_values_bytes)
        plt.legend()

        fig = plt.gcf()
        fig.set_size_inches(4, 3)
        fig.savefig(f"{experiment_dir}{experiment_name}_F1.png", bbox_inches="tight")

        for stat in stats.keys():
            plt.close()
            plt.xlabel('Memory (KB)')
            plt.xscale('log', base=2)
            plt.gca().xaxis.set_major_formatter(FuncFormatter(kb_formatter))
            label = stat.capitalize()

            # handle special cases
            if stat in ['fnr', 'fpr', 'mse']:
                label = stat.upper()
            if stat == 'mse':
                plt.yscale('log', base=10)
            if stat == 'fnr':
                plt.gca().yaxis.set_major_formatter(PercentFormatter(1, decimals=1))
            elif stat == 'fpr':
                plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
            elif stat in ['recall', 'precision']:
                ax = plt.gca()
                ax.set_ylim([0, 1])

            plt.ylabel(label)
            for sketch in sketch_names:
                color = 'C1' if 'M1' in sketch else 'C2'
                marker = 'o'
                if '64' in sketch:
                    marker = '+'
                if '128' in sketch:
                    marker = 's'
                if '256' in sketch:
                    marker = 'x'

                plt.plot(memory_values_bytes,
                         [statistics.fmean(recall_list) for recall_list in stats[stat][sketch]],
                         label=sketch_pretty_names[sketch], color=color, marker=marker)
            plt.xticks(memory_values_bytes)
            plt.legend()

            fig = plt.gcf()
            fig.set_size_inches(4, 3)
            fig.savefig(f"{experiment_dir}{experiment_name}_{stat.capitalize()}.png", bbox_inches="tight")  #  , bbox_inches="tight")