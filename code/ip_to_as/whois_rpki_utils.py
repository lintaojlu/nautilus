import datetime
from pathlib import Path

import pickle, time, sys
import requests

import warnings

from tqdm import tqdm

warnings.filterwarnings("ignore")

root_dir = Path(__file__).resolve().parents[2]


def save_rpki_whois_output(rpki_output, ip_version=4, tags='default'):
    save_directory = root_dir / 'stats/ip2as_data/'

    save_directory.mkdir(parents=True, exist_ok=True)

    save_file = save_directory / 'rpki_whois_output_v{}_{}'.format(ip_version, tags)

    with open(save_file, 'wb') as fp:
        pickle.dump(rpki_output, fp)


def load_rpki_whois_output(ip_version=4, tags='default'):
    file_location = root_dir / 'stats/ip2as_data/rpki_whois_output_v{}_{}'.format(ip_version, tags)

    if Path(file_location).exists():
        with open(file_location, 'rb') as fp:
            rpki_output = pickle.load(fp)
        print(f'Loaded the RPKI whois output from {file_location}')
        return rpki_output
    else:
        return {}


def generate_ip2as_for_list_of_ips(ip_version=4, list_of_ips=[], tags='default', args=None):
    # if the pre-saved file exists, load it
    rpki_output = load_rpki_whois_output(ip_version, tags)
    # 从断点处开始继续
    list_of_ips = list(set(list_of_ips) - set(rpki_output.keys()))
    print(f'Load the RPKI whois output from the file, and continue from the breakpoint. {len(list_of_ips)} IPs left')

    if len(list_of_ips) == 0:
        print(f'Invalid list passed. Pass valid list of IPs')
        return None

    for count, ip_address in tqdm(enumerate(list_of_ips), desc='RPKI whois'):
        try:
            url = f'https://rest.bgp-api.net/api/v1/prefix/{ip_address}/32/search'
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # Check for HTTP errors
            data = response.json()
            result = data.get('result', {})
            meta = result.get('meta', [])
            asns = []

            for entry in meta:
                if entry.get('sourceType') == 'bgp':
                    asns.extend(entry.get('originASNs', []))

            if asns:
                rpki_output[ip_address] = asns
            else:
                continue
        except requests.exceptions.RequestException as e:
            print(f'Network error processing IP {ip_address}: {str(e)}')
        except Exception as e:
            print(f'Error processing IP {ip_address}: {str(e)}')

        if count % 1000 == 0:
            save_rpki_whois_output(rpki_output, ip_version, tags)

    save_rpki_whois_output(rpki_output, ip_version, tags)

    return rpki_output


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print(start_time)
    mode = 1
    ip_version = 4
    args = {'chromedriver_location': root_dir / 'chromedriver', 'ip_version': ip_version, 'tags': 'default'}
    # list_of_ips = ['66.85.82.9', '156.225.182.1', '67.59.254.241', '103.78.227.1', '193.34.197.140', '23.111.226.1',
    #                '193.0.214.1', '152.255.147.235', '216.19.218.1']

    if mode == 0:
        args['num_parallel'] = int(sys.argv[1])
        args['max_ips_to_process'] = int(sys.argv[2])
        ip_version = sys.argv[2]
        args['ips_file_location'] = root_dir / 'stats/mapping_outputs/all_ips_v{}'.format(ip_version)
        args['chromedriver_binary'] = input('Enter chromedriver binary full path: ')
        in_chunks = True
    else:
        with open(root_dir / f'stats/mapping_outputs/all_ips_v{ip_version}', 'rb') as fp:
            list_of_ips = pickle.load(fp)
        in_chunks = False

    rpki_output = generate_ip2as_for_list_of_ips(ip_version, list_of_ips, args=args)
    print(rpki_output)
    print(f'We got results for {len(rpki_output)} IPs')
    print(f'Time taken: {datetime.datetime.now() - start_time}')
