from scapy.all import *

caida_directory_path = '/Users/srani/Documents/roy/caida/'
caida_16_1_file_path = caida_directory_path + 'equinix-chicago.dirA.20160121-125911.UTC.anon.pcap'
if __name__ == '__main__':
    counter = 0
    f = open("caida_16_part1_parsed.txt", "a")
    for p in PcapReader(caida_16_1_file_path):
        f.write(p.sprintf('%IP.src%,%IP.dst%\n'))
        counter += 1
        # if counter % 100000 == 0:
        #     print(counter)
    f.close()
    print(f'Finished parsing. Total packet count: {counter}')
