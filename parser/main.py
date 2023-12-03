from scapy.all import *
from os import listdir
from collections import Counter

caida_year = '18'

parsed_traces_directory = f'../parsed_traces/caida_{caida_year}/'
raw_traces_directory = f'../raw_traces/caida_{caida_year}/'


def make_flow_id(packet):
    key = f'{packet[IP].src},{packet[IP].dst},{packet[IP].proto}'
    if TCP in packet:
        key = f'{key},{packet[TCP].sport},{packet[TCP].dport}'
    elif UDP in packet:
        key = f'{key},{packet[UDP].sport},{packet[UDP].dport}'
    return key


def calculate_trace_statistics():
    counter = Counter()
    for file_name in listdir(parsed_traces_directory):
        with open(parsed_traces_directory + file_name) as file:
            data = [line.rstrip() for line in file]
            counter += Counter(data)
    total_len = sum(counter.values())
    print(f'Trace: CAIDA {caida_year}\'')
    print(f'Total trace length is {total_len}')
    for theta in [1/1000, 1/2000, 1/10000]:
        print(f'Number of HH for theta={theta} is', len([c for (_, c) in counter.most_common() if c >= (total_len*theta)]))


if __name__ == '__main__':
    # calculate_trace_statistics()
    trace_lengths = []
    for (index, raw_file_name) in enumerate(sorted(listdir(raw_traces_directory))):
        with open(parsed_traces_directory + f'parsed_caida{caida_year}_part{index}.txt', 'w') as parsed_file:
            count = 0
            for p in PcapReader(raw_traces_directory + raw_file_name):
                if p is None:
                    print('PcapReader is empty.')
                    break
                if IP not in p:
                    continue
                flow_id = make_flow_id(p)
                parsed_file.write(flow_id + '\n')
                count += 1
        trace_lengths.append(count)
    print('Finished parsing.')
    for (i, l) in enumerate(trace_lengths):
        print(f'Parsed trace #{i} length is', l)
    print(f'Total packet count: {sum(trace_lengths)}')


