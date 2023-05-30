import json

import matplotlib.pyplot as plt
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

# theta = 2000

# stats = {
#     2000: {
#         FPR: {
#             CMSIS_M0: 0.05345,
#             CMSIS_M1: 0.04522,
#             CMSIS_M2: 0.03585,
#             CMSIS_M3: 0.02480,
#         },
#         FNR: {
#             CMSIS_M0: 0,
#             CMSIS_M1: 0.0013,
#             CMSIS_M2: 0.01225,
#             CMSIS_M3: 0.05371
#         }
#     },
#     1000: {
#         FPR: {
#             CMSIS_M0: 0.03463,
#             CMSIS_M1: 0.03273,
#             CMSIS_M2: 0.02914,
#             CMSIS_M3: 0.02890,
#         },
#         FNR: {
#             CMSIS_M0: 0,
#             CMSIS_M1: 0.00013,
#             CMSIS_M2: 0.00149,
#             CMSIS_M3: 0.01247
#         }
#     },
# }

if __name__ == '__main__':
    for stat_file in listdir(stats_directory):
        with open(stats_directory + stat_file) as json_file:
            stats = json.load(json_file)
        sketch_names = sorted(stats.keys())
        # set width of bar
        barWidth = 0.4
        # plt.rc('font', size=12)  # controls default text sizes

        fig = plt.subplots(figsize=(6, 3))

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
        plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
        # plt.title(f"Theta={2000}")
        # plt.ylim([0, 0.06])
        plt.legend()
        plt.savefig(f'model_sim_{stat_file}.png')
        # plt.show()