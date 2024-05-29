from pathlib import Path

import pickle, subprocess, time, sys

import warnings

from tqdm import tqdm
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

warnings.filterwarnings("ignore")
root_dir = Path(__file__).resolve().parents[2]


def save_radb_whois_output(radb_output, ip_version=4, tags='default'):
    save_directory = root_dir / 'stats/ip2as_data/'

    save_directory.mkdir(parents=True, exist_ok=True)

    save_file = save_directory / 'radb_whois_output_v{}_{}'.format(ip_version, tags)

    with open(save_file, 'wb') as fp:
        pickle.dump(radb_output, fp)


def load_radb_whois_output(args, in_chunks, ips_list=[], tags='default'):
    file_location = root_dir / 'stats/ip2as_data/radb_whois_output_{}'.format(tags)

    if Path(file_location).exists():
        with open(file_location, 'rb') as fp:
            radb_output = pickle.load(fp)
    else:
        if len(ips_list) > 0:
            radb_output = generate_ip2as_for_list_of_ips(ips_list, tags, args, in_chunks=in_chunks)
        else:
            print(f'Please enter either valid file tag or ips list')
            return None

    return radb_output


def generate_ip2as_for_list_of_ips(ip_version=4, list_of_ips=[], tags='default', args=None, in_chunks=True):
    if in_chunks:
        num_parallel = args.get('num_parallel', None)
        max_ips_to_process = args.get('max_ips_to_process', None)
        whois_cmd_location = args.get('whois_cmd_location', None)

        all_ips_file = args.get('ips_file_location', None)

        with open(all_ips_file, 'rb') as fp:
            all_ips = pickle.load(fp)

        all_ips = list(all_ips)

        if num_parallel * max_ips_to_process > len(all_ips):
            list_of_ips = all_ips[(num_parallel - 1) * max_ips_to_process:]
        else:
            list_of_ips = all_ips[(num_parallel - 1) * max_ips_to_process: num_parallel * max_ips_to_process]

    else:
        whois_cmd_location = args.get('whois_cmd_location', None)

    radb_output = {}

    with ThreadPoolExecutor(max_workers=1) as executor:
        future_to_ip = {executor.submit(whois_query, ip, whois_cmd_location): ip for ip in list_of_ips}
        for count, future in tqdm(enumerate(as_completed(future_to_ip)), desc='RADB whois', total=len(list_of_ips)):
            ip, list_out = future.result()
            ip, results = parse_whois_output(ip, list_out)
            if results:
                ip_to_as_map_result = radb_output.get(ip, [])
                ip_to_as_map_result.extend(results)
                radb_output[ip] = ip_to_as_map_result

            if count % 1000 == 0:
                save_radb_whois_output(radb_output, ip_version, tags)

    print(f'Doing a final save with {len(radb_output)} IPs processed')
    save_radb_whois_output(radb_output, ip_version, tags)
    return radb_output


def whois_query(ip, whois_cmd_location):
    cmd_str = f'{whois_cmd_location} -h whois.radb.net {ip}'
    proc = subprocess.Popen(cmd_str, stdout=subprocess.PIPE, shell=True)
    out, _ = proc.communicate()
    return ip, out.decode(errors='ignore').split('\n')


def parse_whois_output(ip, list_out):
    results = []
    for item in list_out:
        if 'AS' in item and ('origin:' in item.lower() or 'aut-num:' in item.lower()):
            try:
                result = item.split(":")[1].strip()
                if result != '':
                    results.append(result)
            except:
                continue
    return ip, results


args = {'whois_cmd_location': '/home/lintao/anaconda3/envs/ki3/bin/whois'}

if __name__ == '__main__':
    mode = 1
    ip_version = 4

    if mode == 0:
        args['num_parallel'] = int(sys.argv[1])
        args['max_ips_to_process'] = int(sys.argv[2])
        ip_version = sys.argv[3]
        args['ips_file_location'] = root_dir / 'stats/mapping_outputs/all_ips_v{}'.format(ip_version)
    else:
        with open(root_dir / f'stats/mapping_outputs/all_ips_v{ip_version}', 'rb') as fp:
            list_of_ips = pickle.load(fp)
    # list_of_ips = ['66.85.82.9', '156.225.182.1', '67.59.254.241', '103.78.227.1', '193.34.197.140', '23.111.226.1',
    #                '193.0.214.1', '152.255.147.235', '216.19.218.1']

    import random


    def generate_random_ipv4():
        return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"


    list_of_ips = [generate_random_ipv4() for _ in range(100)]

    if mode == 0:
        in_chunks = True
    else:
        in_chunks = False

    radb_output = generate_ip2as_for_list_of_ips(ip_version, list_of_ips, args=args, in_chunks=in_chunks)

    print(f'We got results for {len(radb_output)} IPs')
