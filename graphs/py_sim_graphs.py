import json
import os

import matplotlib
from matplotlib.ticker import FuncFormatter, PercentFormatter
from matplotlib.pyplot import figure
import statistics

import matplotlib.pyplot as plt
from os import listdir

stats_directory = "./statistics/python/"
fcm_topk_stats_directory = stats_directory + 'fcm-topk/'

output_base_dir = "./pysim_graphs/"

memory_values_bytes = [16 * 1024 * (2 ** i) for i in range(7)]


def kb_formatter(val, pos):
    return f'{int(val // 1024)}'


if __name__ == '__main__':
    # matplotlib.rcParams.update({'font.size': 11})
    for stat_file in sorted([file for file in listdir(stats_directory) if 'caida18' in file]):
        experiment_name = stat_file.split('.')[0]
        experiment_dir = output_base_dir + experiment_name + '/'
        if not os.path.exists(experiment_dir):
            os.makedirs(experiment_dir)

        # loading the FCM+TopK stats separately
        fcm_topk_stats_file = 'caida18_th1k_fcm-topk.json' if 'th1k' in stat_file else 'caida18_th2k_fcm-topk.json'
        with open(fcm_topk_stats_directory + fcm_topk_stats_file) as json_file:
            fcm_topk_stats = json.load(json_file)

        with open(stats_directory + stat_file) as json_file:
            stats = json.load(json_file)
        for stat in stats.keys():  # adding the statistics of FCM+TopK from their file
            stats[stat]['FCM+TopK'] = fcm_topk_stats[stat]['FCM+TopK']

        sketch_names = sorted(stats['recall'].keys())
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
            mem_vals = memory_values_bytes
            stat_vals = f1[sketch]
            if sketch == 'FCM+TopK':
                mem_vals = [128 * 1024 * (2 ** i) for i in range(4)]
                stat_vals = f1[sketch][len(memory_values_bytes)-len(mem_vals):]
            plt.plot(mem_vals,
                     stat_vals,
                     label=sketch_pretty_names[sketch])
        plt.xticks(memory_values_bytes)
        plt.legend()

        fig = plt.gcf()
        fig.set_size_inches(4, 3)
        fig.subplots_adjust(left=0.16, right=0.99, top=0.98, bottom=0.15)
        fig.savefig(f"{experiment_dir}{experiment_name}_F1.png")

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

            plt.ylabel(label)

            for sketch in sketch_names:
                mem_vals = memory_values_bytes
                stat_vals = stats[stat][sketch]
                if sketch == 'FCM+TopK':
                    mem_vals = [128 * 1024 * (2 ** i) for i in range(4)]
                    stat_vals = stats[stat][sketch][len(memory_values_bytes) - len(mem_vals):]
                plt.plot(mem_vals,
                         [statistics.fmean(stat_list) for stat_list in stat_vals],
                         label=sketch_pretty_names[sketch])

            if stat == 'recall':
                ax = plt.gca()
                ax.set_ylim(bottom=0)
            plt.xticks(memory_values_bytes)
            plt.legend()

            fig = plt.gcf()
            fig.set_size_inches(4, 3)
            fig.subplots_adjust(left=0.16, right=0.99, top=0.98, bottom=0.15)
            fig.savefig(f"{experiment_dir}{experiment_name}_{stat.capitalize()}.png")  #  , bbox_inches="tight")