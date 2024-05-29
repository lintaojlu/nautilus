import datetime
from pathlib import Path

import pickle, time, sys
import requests

import warnings

warnings.filterwarnings("ignore")

root_dir = Path(__file__).resolve().parents[2]


def save_rpki_whois_output(rpki_output, ip_version=4, tags='default'):
    save_directory = root_dir / 'stats/ip2as_data/'

    save_directory.mkdir(parents=True, exist_ok=True)

    save_file = save_directory / 'rpki_whois_output_v{}_{}'.format(ip_version, tags)

    with open(save_file, 'wb') as fp:
        pickle.dump(rpki_output, fp)


def generate_ip2as_for_list_of_ips(ip_version=4, list_of_ips=[], tags='default', args=None, in_chunks=False):
    rpki_output = {}

    if len(list_of_ips) == 0:
        print(f'Invalid list passed. Pass valid list of IPs')
        return None

    for count, ip_address in enumerate(list_of_ips):
        try:
            url = f'https://rest.bgp-api.net/api/v1/prefix/{ip_address}/32/search'
            response = requests.get(url)
            data = response.json()
            result = data.get('result', {})
            meta = result.get('meta', [])
            asns = []

            for entry in meta:
                if entry.get('sourceType') == 'bgp':
                    asns.extend(entry.get('originASNs', []))

            if asns:
                print(f'IP address is {ip_address} and ASNs are {asns}')
                rpki_output[ip_address] = asns
            else:
                print(f'No AS information found for IP {ip_address}')
        except Exception as e:
            print(f'Error processing IP {ip_address}: {str(e)}')

        if count % 100 == 0:
            print(f'Doing a partial save at count {count}')
            save_rpki_whois_output(rpki_output, ip_version, tags)

    print(f'Doing a final save with {len(rpki_output)} IPs processed')
    save_rpki_whois_output(rpki_output, ip_version, tags)

    return rpki_output


def load_rpki_whois_output(args, in_chunks, ips_list=None, tags='default'):
    if ips_list is None:
        ips_list = []
    file_location = root_dir / 'stats/ip2as_data/rpki_whois_output_{}'.format(tags)

    if Path(file_location).exists():
        with open(file_location, 'rb') as fp:
            rpki_output = pickle.load(fp)
    else:
        if len(ips_list) > 0:
            rpki_output = generate_ip2as_for_list_of_ips(ips_list, tags, args, in_chunks=in_chunks)
        else:
            print(f'Please enter either valid file tag or ips list')
            return None

    return rpki_output


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print(start_time)
    mode = 1
    args = {}
    ip_version = 4
    list_of_ips = ['66.85.82.9', '156.225.182.1', '67.59.254.241', '103.78.227.1', '193.34.197.140', '23.111.226.1',
                   '193.0.214.1', '152.255.147.235', '216.19.218.1']

    if mode == 0:
        args['num_parallel'] = int(sys.argv[1])
        args['max_ips_to_process'] = int(sys.argv[2])
        ip_version = sys.argv[2]
        args['ips_file_location'] = root_dir / 'stats/mapping_outputs/all_ips_v{}'.format(ip_version)
        args['chromedriver_binary'] = input('Enter chromedriver binary full path: ')
        args['chromedriver_location'] = root_dir / 'chromedriver'
        in_chunks = True
    else:
        args['chromedriver_location'] = root_dir / 'chromedriver'
        # with open(root_dir / f'stats/mapping_outputs/all_ips_v{ip_version}', 'rb') as fp:
        #     list_of_ips = pickle.load(fp)
        in_chunks = False

    rpki_output = generate_ip2as_for_list_of_ips(ip_version, list_of_ips, args=args, in_chunks=in_chunks)
    print(rpki_output)
    print(f'We got results for {len(rpki_output)} IPs')
    print(f'Time taken: {datetime.datetime.now() - start_time}')
