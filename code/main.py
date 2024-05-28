import os
import pickle
from collections import namedtuple
from datetime import datetime
from pathlib import Path

from traceroute import ripe_traceroute_utils
from utils import traceroute_utils
from location import ripe_geolocation_utils, caida_geolocation_utils, maxmind_utils, ipgeolocation_utils
from ip_to_as import whois_radb_utils, whois_rpki_utils, cymru_whois_utils, whois_itdk_utils

Location = namedtuple('Location', ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude',
                                   'autonomous_system_number', 'network', 'ISP', 'Org'])
MaxmindLocation = namedtuple('MaxmindLocation',
                             ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude',
                              'autonomous_system_number', 'network'])
root_dir = Path(__file__).resolve().parents[1]

if __name__ == '__main__':
    # The measurement ids used are 5051 and 5151 for v4 and 6052 and 6152 for v6
    # The number (6) in example below indicates the IP version, which will be according to the measurement IDs
    # That number is mostly used for just saving the results
    start_time = datetime(2024, 5, 1, 0)
    end_time = datetime(2024, 5, 2, 0)
    ip_version = 4
    in_chunks = False
    args = {'chromedriver_location': root_dir / 'chromedriver', 'ip_version': ip_version}

    print('******* Generating the traceroutes *******')
    msm_ids = ['5051', '5151']
    for msm_id in msm_ids:
        result = ripe_traceroute_utils.ripe_process_traceroutes(start_time, end_time, msm_id, ip_version, False)
        print(f'Result length for {msm_id} is {len(result)}')

    print('******* Generating all the unique IPs and links *******')
    traceroute_utils.load_all_links_and_ips_data(ip_version=ip_version)

    print('******* Generating geolocation results *******')
    with open(root_dir / f'stats/mapping_outputs/all_ips_v{ip_version}', 'rb') as fp:
        sample_ips_list = pickle.load(fp)

    print('1. RIPE geolocation')
    ripe_location = ripe_geolocation_utils.generate_location_for_list_of_ips_ripe(sample_ips_list, ip_version)
    print(f'RIPE: Results for {len(ripe_location)} IPs')

    print('2. CAIDA geolocation')
    caida_location = caida_geolocation_utils.generate_location_for_list_of_ips(sample_ips_list)
    print(f'CAIDA: Results for {len(caida_location)} IPs')

    print('3. Maxmind geolocation')
    maxmind_location, skipped_ips = maxmind_utils.generate_locations_for_list_of_ips(sample_ips_list, ip_version)
    print(f'Maxmind: Results for {len(maxmind_location)} IPs')

    print('4. IPGeolocation geolocation')
    ipgeolocation_utils.generate_location_for_list_of_ips(sample_ips_list, in_chunks=False, args=args, len_single_file=4500)

    print('******* SoL validation *******')
    for msm_id in msm_ids:
        result = ripe_traceroute_utils.ripe_process_traceroutes(start_time, end_time, msm_id, ip_version, True)
        print(f'Result length for {msm_id} is {len(result)}')

    print('******* IP to AS mapping *******')
    with open(root_dir / f'stats/mapping_outputs/all_ips_v{ip_version}', 'rb') as fp:
        list_of_ips = pickle.load(fp)

    print('1. CAIDA ITDK queries')
    caida_output = whois_itdk_utils.generate_ip2as_for_list_of_ips(ip_version, list_of_ips, 'default')
    print(f'CAIDA: Results for {len(caida_output)} IPs')

    print('2. Cymru queries')
    cymru_output = cymru_whois_utils.generate_ip2as_for_list_of_ips(list_of_ips, ip_version, 'default')
    print(f'Cymru: Results for {len(cymru_output)} IPs')

    print('3. RPKI queries')
    rpki_output = whois_rpki_utils.generate_ip2as_for_list_of_ips(ip_version, list_of_ips, args=args, in_chunks=in_chunks)
    print(f'RPKI: Results for {len(rpki_output)} IPs')

    print('4. RADB queries')
    radb_output = whois_radb_utils.generate_ip2as_for_list_of_ips(ip_version, list_of_ips, args=args, in_chunks=in_chunks)
    print(f'RADB: Results for {len(radb_output)} IPs')





