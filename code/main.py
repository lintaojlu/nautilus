import argparse
import os
import pickle
from collections import namedtuple
from datetime import datetime
from pathlib import Path
from multiprocessing import Process

from traceroute import ripe_traceroute_utils
from utils import traceroute_utils, common_utils, merge_data
from location import ripe_geolocation_utils, caida_geolocation_utils, maxmind_utils, ipgeolocation_utils, \
    ip2location_geolocation_utils
from ip_to_as import whois_radb_utils, whois_rpki_utils, cymru_whois_utils, whois_itdk_utils

Location = namedtuple('Location', ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude',
                                   'autonomous_system_number', 'network', 'ISP', 'Org'])
MaxmindLocation = namedtuple('MaxmindLocation',
                             ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude',
                              'autonomous_system_number', 'network'])
LandingPoints = namedtuple('LandingPoints', ['latitude', 'longitude', 'country', 'location', 'cable'])
Cable = namedtuple('Cable', ['name', 'landing_points', 'length', 'owners', 'notes', 'rfs', 'other_info'])

root_dir = Path(__file__).resolve().parents[1]


def process_ripe_geolocation(ips_list, ip_version, suffix):
    print('1. RIPE geolocation')
    ripe_location = ripe_geolocation_utils.generate_location_for_list_of_ips_ripe(ips_list, ip_version, tags=suffix)
    print(f'RIPE: Results for {len(ripe_location)} IPs')


def process_caida_geolocation(ips_list, suffix):
    print('2. CAIDA geolocation')
    caida_location = caida_geolocation_utils.generate_location_for_list_of_ips(ips_list, tags=suffix)
    print(f'CAIDA geo: Results for {len(caida_location)} IPs')


def process_maxmind_geolocation(ips_list, ip_version, suffix):
    print('3. Maxmind geolocation')
    maxmind_location, skipped_ips = maxmind_utils.generate_locations_for_list_of_ips(ips_list, ip_version, tags=suffix)
    print(f'Maxmind: Results for {len(maxmind_location)} IPs')


def process_ipgeolocation_geolocation(ips_list, args, suffix):
    print('4. IPGeolocation geolocation(use ip2location to replace)')
    ip2location_location = ip2location_geolocation_utils.locate_ips_by_ip2location(ips_list, args)
    print(f'IPGeolocation: Results for {len(ip2location_location)} IPs')
    # ipgeolocation_utils.generate_location_for_list_of_ips(ips_list, args=args)
    # print('******* Merging the geolocation results *******')
    # merge_data.common_merge_operation(root_dir / 'stats/location_data/iplocation_files', 2, [], ['ipgeolocation_file_'],
    #                                   True,
    #                                   f'iplocation_location_output_v{ip_version}_{suffix}')


def process_caida_ip_to_as(ip_version, list_of_ips, suffix):
    print('1. CAIDA ITDK queries')
    caida_output = whois_itdk_utils.generate_ip2as_for_list_of_ips(ip_version, list_of_ips, suffix)
    print(f'CAIDA ip2as: Results for {len(caida_output)} IPs')


def process_cymru_ip_to_as(ip_version, list_of_ips, suffix):
    print('2. Cymru queries')
    cymru_output = cymru_whois_utils.generate_ip2as_for_list_of_ips(list_of_ips, ip_version, suffix)
    print(f'Cymru: Results for {len(cymru_output)} IPs')


def process_radb_ip_to_as(ip_version, list_of_ips, args, suffix):
    print('3. RADB queries')
    args['whois_cmd_location'] = '/home/lintao/anaconda3/envs/ki3/bin/whois'
    radb_output = whois_radb_utils.generate_ip2as_for_list_of_ips(ip_version, list_of_ips, args=args, tags=suffix)
    print(f'RADB: Results for {len(radb_output)} IPs')


def process_rpki_ip_to_as(ip_version, list_of_ips, args, suffix):
    print('4. RPKI queries')
    rpki_output = whois_rpki_utils.generate_ip2as_for_list_of_ips(ip_version, list_of_ips, args=args, tags=suffix)
    print(f'RPKI: Results for {len(rpki_output)} IPs')


# The measurement ids used are 5051 and 5151 for v4 and 6052 and 6152 for v6
# The number (6) in example below indicates the IP version, which will be according to the measurement IDs
# That number is mostly used for just saving the results


def main(args):
    start = input("Input the start time and end time(yyyy-mm-dd):")
    end = input("Input the end time(yyyy-mm-dd):")
    suffix = start + '_' + end

    start_time = datetime.strptime(start, '%Y-%m-%d')
    end_time = datetime.strptime(end, '%Y-%m-%d')
    ip_version = 4
    args_dict = {'chromedriver_location': root_dir / 'chromedriver', 'ip_version': ip_version, 'tags': suffix}
    msm_ids = ['5051', '5151']

    print("******* Nautilus *******")
    print(f"******* {start_time} - {end_time} *******")

    if not args.no_traceroute:
        print('******* Generating the traceroutes *******')
        for msm_id in msm_ids:
            result = ripe_traceroute_utils.ripe_process_traceroutes(start_time, end_time, msm_id, ip_version, False,
                                                                    suffix=suffix)

    print('******* Generating/getting all the unique IPs and links *******')
    links, ips_list = traceroute_utils.load_all_links_and_ips_data(ip_version=ip_version, suffix=suffix)
    print(f'Unique IPs: {len(ips_list)}')
    print(f'Unique Links: {len(links)}')

    processes = []

    if not args.no_ipgeo:
        print('******* Starting geolocation processes *******')
        processes.append(Process(target=process_ripe_geolocation, args=(ips_list, ip_version, suffix)))
        processes.append(Process(target=process_caida_geolocation, args=(ips_list, suffix)))
        processes.append(Process(target=process_maxmind_geolocation, args=(ips_list, ip_version, suffix)))
        processes.append(Process(target=process_ipgeolocation_geolocation, args=(ips_list, args_dict, suffix)))

    if not args.no_ip2as:
        print('******* Starting IP to AS mapping processes *******')
        processes.append(Process(target=process_caida_ip_to_as, args=(ip_version, ips_list, suffix)))
        processes.append(Process(target=process_cymru_ip_to_as, args=(ip_version, ips_list, suffix)))
        processes.append(Process(target=process_rpki_ip_to_as, args=(ip_version, ips_list, args_dict, suffix)))
        processes.append(Process(target=process_radb_ip_to_as, args=(ip_version, ips_list, args_dict, suffix)))

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    print('******* SoL validation *******')
    for msm_id in msm_ids:
        result = ripe_traceroute_utils.ripe_process_traceroutes(start_time, end_time, msm_id, ip_version, True,
                                                                suffix=suffix, update_probe_info=True)
        print(f'Result length for {msm_id} is {len(result)}')

    print('******* Nautilus Mapping *******')
    mode = 2

    print("Generate an initial mapping for each category")
    common_utils.generate_cable_mapping(mode=mode, ip_version=ip_version, sol_threshold=0.05, suffix=suffix)
    print("Merge mapping results of multiple experiments for each category")
    merge_data.common_merge_operation(root_dir / f'stats/mapping_outputs_{suffix}', 1, [], ['v4'], True, None)
    print("Merging the results for all categories")
    common_utils.generate_final_mapping(mode=mode, ip_version=ip_version, threshold=0.05)
    print("Re-updating the categories map")
    common_utils.regenerate_categories_map(mode=mode, ip_version=ip_version, suffix=suffix)
    print("Done")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Control the execution of traceroute, geolocation, and IP to AS mapping.')
    parser.add_argument('--no_traceroute', action='store_true', help='Skip generating the traceroutes')
    parser.add_argument('--no_ipgeo', action='store_true', help='Skip geolocation processes')
    parser.add_argument('--no_ip2as', action='store_true', help='Skip IP to AS mapping processes')

    args = parser.parse_args()
    main(args)
