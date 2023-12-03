import json

import matplotlib.pyplot as plt
from matplotlib import ticker
from matplotlib.ticker import PercentFormatter
import numpy as np
from os import listdir

# Sketch Names
CMSIS_M0 = 'CMSIS-M0'
CMSIS_M1 = 'CMSIS-M1'
CMSIS_M2 = 'CMSIS-M2'
CMSIS_M3 = 'CMSIS-M3'
# Statistics Names
FNR = 'fnr'
FPR = 'fpr'

stats_directory = "./statistics/model/"
output_base_dir = "./model_graphs/"

if __name__ == '__main__':
    for stat_file in listdir(stats_directory):
        with open(stats_directory + stat_file) as json_file:
            stats = json.load(json_file)
        sketch_names = sorted(stats.keys())
        # set width of bar
        barWidth = 0.4
        # plt.rc('font', size=12)  # controls default text sizes

        fig = plt.subplots(figsize=(4, 2))

        fpr = [stats[sketch][FPR] for sketch in sketch_names]
        fnr = [stats[sketch][FNR] for sketch in sketch_names]

        # Set position of bar on X axis
        br1 = np.arange(len(fpr))
        br2 = [x + barWidth for x in br1]
        # Make the plot
        plt.bar(br1, fpr, color='b', width=barWidth, label='FPR')
        plt.bar(br2, fnr, color='r', width=barWidth, label='FNR')
        # Adding Xticks
        plt.xticks([r + barWidth / 2 for r in range(len(sketch_names))],
                   [sketch for sketch in sketch_names])

        if 'caida18' in stat_file and 'th2k' not in stat_file:
            plt.gca().set_yticks(plt.gca().get_yticks()[::2])
            # plt.gca().yaxis.set_major_locator(ticker.MultipleLocator(1))
        plt.gca().yaxis.set_major_formatter(PercentFormatter(1, decimals=0))
        # plt.title(f"Theta={2000}")
        # plt.ylim([0, 0.06])
        plt.legend()
        plt.savefig(output_base_dir + f'model_sim_{stat_file}.png', bbox_inches="tight")
        # plt.show()