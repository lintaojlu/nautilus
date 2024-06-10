import os
import pickle
from collections import namedtuple
from datetime import datetime
from pathlib import Path

from traceroute import ripe_traceroute_utils
from utils import traceroute_utils, common_utils, merge_data
from location import ripe_geolocation_utils, caida_geolocation_utils, maxmind_utils, ipgeolocation_utils
from ip_to_as import whois_radb_utils, whois_rpki_utils, cymru_whois_utils, whois_itdk_utils

Location = namedtuple('Location', ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude',
                                   'autonomous_system_number', 'network', 'ISP', 'Org'])
MaxmindLocation = namedtuple('MaxmindLocation',
                             ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude',
                              'autonomous_system_number', 'network'])
root_dir = Path(__file__).resolve().parents[1]

if __name__ == '__main__':


    print("******* Nautilus Mapping *******")
    print(f"******* {start_time} - {end_time} *******")

    print(datetime.now(), '******* Generating the traceroutes *******')
    for msm_id in msm_ids:
        result = ripe_traceroute_utils.ripe_process_traceroutes(start_time, end_time, msm_id, ip_version, False)
    print(datetime.now(), '******* Generating all the unique IPs and links *******')
    links, uniq_ips_list = traceroute_utils.load_all_links_and_ips_data(ip_version=ip_version)
    print(f'Unique IPs: {len(uniq_ips_list)}')
    print(f'Unique Links: {len(links)}')