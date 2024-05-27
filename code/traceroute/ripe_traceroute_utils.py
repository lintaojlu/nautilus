import json
import os, sys
from pathlib import Path

import calendar
from datetime import datetime, timezone, timedelta

import requests
import pickle

from collections import namedtuple

from ipaddress import ip_network, ip_address

from tqdm import tqdm

root_dir = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(root_dir))

TraceRoute = namedtuple('TraceRoute', ['hops', 'other_info'])

Hops = namedtuple('Hops', ['hop', 'ip_address', 'rtt'])

private_ranges = [ip_network("192.168.0.0/16"), ip_network("10.0.0.0/8"), ip_network("172.16.0.0/12")]

private_ranges_v6 = [ip_network("fc00::/7"), ip_network("fc00::/8"), ip_network("fd00::/8")]

from code.traceroute.ripe_probe_location_info import load_probe_location_result
from code.traceroute.geolocation_latency_based_validation_common_utils import load_all_geolocation_info, \
    extract_latlon_and_perform_sol_test, fill_locations_dict_scores

# Once location scripts are done, load directly from those
Location = namedtuple('Location', ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude',
                                   'autonomous_system_number', 'network', 'ISP', 'Org'])
MaxmindLocation = namedtuple('MaxmindLocation',
                             ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude',
                              'autonomous_system_number', 'network'])


def download_data_from_ripe_atlas(start_time, end_time, msm_id):
    """
    This function essentially downloads the data from RIPE Atlas website and saves the
    result in the stats directory
    Inputs
        start_time -> The start time to collect the data (should be in datetime format)
        end_time -> The end time for the traceroutes to be collected (should be in datetime format)
        return_content -> By default 0, if set to 1, we return the last processed traceroutes
    """
    save_directory = root_dir / 'stats' / 'ripe_data'

    if not os.path.exists(save_directory):
        os.makedirs(save_directory, exist_ok=True)

    if start_time < datetime(2016, 5, 8, 0, 0, 0):
        print(f"Traceroute data is before 2016-05-08")
        return

    file_name = 'raw_output_' + msm_id + '_' + start_time.strftime("%Y_%m_%d") + '_' + end_time.strftime("%Y_%m_%d") + '.txt'
    file_path = save_directory / file_name

    start_time_int = int(calendar.timegm(start_time.timetuple()))
    # if end_time is end with 00:00:00, reduce 1 second
    if end_time.hour == 0 and end_time.minute == 0 and end_time.second == 0:
        end_time_int = int(calendar.timegm(end_time.timetuple())) - 1
    else:
        end_time_int = int(calendar.timegm(end_time.timetuple()))

    print(f'Downloading/Loading ripe {msm_id} traceroutes: start_time: {start_time}, end_time: {end_time}')

    retries = 0
    max_retries = 10
    while retries < max_retries:
        if os.path.isfile(file_path):
            # Check last line of the file for valid JSON format
            with open(file_path, 'r') as f:
                try:
                    last_line = f.readlines()[-1]
                    json.loads(last_line)
                    print(f"{start_time}'s traceroute file is complete")
                    response_list = []
                    with open(file_path, 'r') as fp:
                        for line in fp:
                            response_list.append(json.loads(line))

                    print("Finished processing all the traceroutes in the given data range.")

                    return response_list, file_path
                except Exception as e:
                    print(f"{start_time}'s traceroute file is incomplete, retrying...")
                    retries += 1

        url = f"https://atlas.ripe.net/api/v2/measurements/{msm_id}/results/?start={start_time_int}&stop={end_time_int}&format=txt"
        print(f'url: {url}')
        response = requests.get(url, stream=True)
        if response.status_code == 200:  # 200 OK
            mode = 'wb'  # 写模式
            with open(file_path, mode) as f:
                try:
                    for chunk in tqdm(response.iter_content(chunk_size=1024)):
                        f.write(chunk)
                    print(f"Downloaded {start_time}'s traceroute")
                except Exception as e:
                    retries += 1
        else:
            print(f"Failed, HTTP code: {response.status_code}")
            retries += 1

    if retries == max_retries:
        print(f"Failed to download {start_time}'s traceroute after {max_retries} attempts")


def process_transform_traceroute(traceroute_data, save_file, return_content=0):
    """
    This function essentially extracts required portions of the traceroute, removes hops with
    non-useful entries and saves the output to the stats directory
    Input
        traceroute_data -> List of all traceroutes (Essentially pass the output from download_data_from_ripe_atlas or read raw_output_*** file)
        save_file -> The file to which the processed output should be written
    """

    output_traceroute = []
    skipped_traceoute = []
    count = 1

    if Path(save_file).exists():
        with open(save_file, 'rb') as fp:
            print('Directly loading file from saved locations')
            output_traceroute = pickle.load(fp)

    else:
        for traceroute in traceroute_data:
            try:
                hops = {0: [Hops(0, traceroute['src_addr'], 0)]}
                for hop in traceroute['result']:
                    ips_to_rtts = []
                    hop_num = hop['hop']
                    for i in hop['result']:
                        rtt = i.get('rtt', None)
                        ip = i.get('from', None)
                        if ip and rtt:
                            ips_to_rtts.append(Hops(hop_num, ip, rtt))
                    if len(ips_to_rtts) > 0:
                        hops[hop_num] = ips_to_rtts
                        last_rtt = rtt
                hops[256] = [Hops(256, traceroute['dst_addr'], last_rtt)]
                output_traceroute.append(TraceRoute(hops, {'time': datetime.fromtimestamp(traceroute['timestamp']),
                                                           'probe_id': traceroute['prb_id']}))
            except:
                skipped_traceoute.append(traceroute)

            count += 1

        print(f"Skipped traceroutes : {len(skipped_traceoute)}")

        print(f"Total count now is {count}")

        with open(save_file, 'wb') as fp:
            pickle.dump(output_traceroute, fp, protocol=pickle.HIGHEST_PROTOCOL)

    if return_content:
        return output_traceroute


def check_if_ip_is_private(ip, v4=True):
    """
    A simple check if a given IP is in the private IP range or not
    Returns True if in private IP range, else returns False
    """

    if v4:
        if ip_address(ip) in private_ranges[0] or ip_address(ip) in private_ranges[1] or ip_address(ip) in \
                private_ranges[2]:
            return True
        else:
            return False
    else:
        if ip_address(ip) in private_ranges_v6[0] or ip_address(ip) in private_ranges_v6[1] or ip_address(ip) in \
                private_ranges_v6[2]:
            return True
        else:
            return False


def geolocation_sol_validation_ripe(updated_traceroute_output, probe_to_coordinate_map,
                                    iplocation_output, maxmind_output,
                                    ripe_output, caida_output,
                                    ip_location_with_penalty_and_total_count, v4=True):
    for count, traceroute in enumerate(updated_traceroute_output):
        probe = traceroute.other_info['probe_id']
        initial_lat_lon = probe_to_coordinate_map[str(probe)][1]

        for key, contents in traceroute.hops.items():

            # As the same IP could be repeated based on the # of probes sent, let's not look at the huge geolocation sources
            # again and again. Kind of like a local cache
            local_dict = {}

            for hop_info in contents:
                ip = hop_info.ip_address
                rtt = hop_info.rtt

                if ip and not check_if_ip_is_private(ip, v4):

                    if ip not in local_dict.keys():

                        locations = iplocation_output.get(ip, None)
                        maxmind_location = maxmind_output.get(ip, None)
                        ripe_location = ripe_output.get(ip, None)
                        caida_location = caida_output.get(ip, None)

                        # Updating the local cache
                        local_dict[ip] = {'locations': locations, 'maxmind_location': maxmind_location,
                                          'ripe_location': ripe_location, 'caida_location': caida_location}

                    else:

                        locations = local_dict[ip]['locations']
                        maxmind_location = local_dict[ip]['maxmind_location']
                        ripe_location = local_dict[ip]['ripe_location']
                        caida_location = local_dict[ip]['caida_location']

                    prev_examined_location = ip_location_with_penalty_and_total_count.get(ip, None)

                    if locations:
                        for ind, location in enumerate(locations):
                            status, (latitude, longitude) = extract_latlon_and_perform_sol_test(location,
                                                                                                initial_lat_lon, rtt,
                                                                                                ind)
                            fill_locations_dict_scores(prev_examined_location, status,
                                                       latitude, longitude,
                                                       ip, ind, ip_location_with_penalty_and_total_count)

                    if maxmind_location:
                        status, (latitude, longitude) = extract_latlon_and_perform_sol_test(maxmind_location,
                                                                                            initial_lat_lon, rtt, 8)
                        fill_locations_dict_scores(prev_examined_location, status,
                                                   latitude, longitude,
                                                   ip, 8, ip_location_with_penalty_and_total_count)

                    if ripe_location:
                        status, (latitude, longitude) = extract_latlon_and_perform_sol_test(ripe_location,
                                                                                            initial_lat_lon, rtt, 9)
                        fill_locations_dict_scores(prev_examined_location, status,
                                                   latitude, longitude,
                                                   ip, 9, ip_location_with_penalty_and_total_count)

                    if caida_location:
                        status, (latitude, longitude) = extract_latlon_and_perform_sol_test(caida_location,
                                                                                            initial_lat_lon, rtt, 10)
                        fill_locations_dict_scores(prev_examined_location, status,
                                                   latitude, longitude,
                                                   ip, 10, ip_location_with_penalty_and_total_count)


def get_ripe_hops(traceroute, v4=True):
    """
    Get all hops in a given traceroute that satisfy 2 conditions
    (i) Hops should be consecutive (to be considered a link)
    (ii) IP address at both ends of the link should be non-private

    Returns a list of all such hops in a traceroute which satisfy this criteria
    Also returns some counts, which are used for stats later
    """

    prev_item = traceroute.hops[0]
    prev_ip_to_rtt = {prev_item[0].ip_address: [prev_item[0].rtt]}

    actual_count = 0
    conditional_count = 0

    return_hops = []

    for index, (key, item) in enumerate(traceroute.hops.items()):
        if key != 0:
            ip_to_rtt = {}
            hop_num = item[0].hop
            for hop_info in item:
                current_rtt_info = ip_to_rtt.get(hop_info.ip_address, [])
                if hop_info.rtt:
                    current_rtt_info.append(hop_info.rtt)
                    ip_to_rtt[hop_info.ip_address] = current_rtt_info

            for ip, rtt in ip_to_rtt.items():
                current_rtt_max = max(rtt)
                current_rtt_min = min(rtt)

                for prev in prev_item:
                    if prev.hop == (hop_num - 1) and not check_if_ip_is_private(prev.ip_address,
                                                                                v4) and not check_if_ip_is_private(ip,
                                                                                                                   v4):
                        return_hops.append(((prev.ip_address, ip),
                                            (min(prev_ip_to_rtt[prev.ip_address]), current_rtt_min),
                                            (max(prev_ip_to_rtt[prev.ip_address]), current_rtt_max)))
                        conditional_count += 1

            actual_count += len(ip_to_rtt)

            prev_item = item
            prev_ip_to_rtt = ip_to_rtt

    return return_hops, actual_count, conditional_count


def ripe_process_traceroutes(start_time, end_time, msm_id, ip_version, geolocation_validation=False):
    """
    This function puts it all together
    (1) Get the raw traceroutes first
    (2) Process the traceroutes
    (3) Generate a dictionary with links and latencies

    Additionally save the results for every 12 hours

    Returns the generated dictionary
    """

    time = start_time
    d = {}
    count = 0

    if ip_version == 4:
        v4 = True
    else:
        v4 = False
    print(f'Processing traceroutes for {msm_id} and IP version {ip_version}')

    if geolocation_validation:
        ip_location_with_penalty_and_total_count = {}

        print(f'First loading probe to coordinate map')

        probe_to_coordinate_map = load_probe_location_result()

        print('Loading all geolocation sources')

        maxmind_output, ripe_output, caida_output, iplocation_output = load_all_geolocation_info(ip_version)

        print('Successfully loaded all geolocation results')

    while time < end_time:

        print('Stage 1 : Loading/Downloading the data from RIPE Atlas')
        time_end = time + timedelta(hours=24)
        # save_file is like raw_output_5051_current_date_label
        traceroute_output, save_file = download_data_from_ripe_atlas(time, time_end, msm_id)
        print(f'Length of raw traceroutes is {len(traceroute_output)}')

        print('Stage 2 : Processing the data from RIPE Atlas')
        new_file = 'processed_' + '_'.join(save_file.name.split('_')[1:])
        parent_dir = save_file.parent
        processed_file = parent_dir / new_file
        updated_traceroute_output = process_transform_traceroute(traceroute_output, processed_file, 1)
        print(f'Length of processed traceroutes : {len(updated_traceroute_output)}')

        if geolocation_validation:
            print('Stage 2.1: Performing SoL testing and validating locations')

            geolocation_sol_validation_ripe(updated_traceroute_output, probe_to_coordinate_map,
                                            iplocation_output, maxmind_output,
                                            ripe_output, caida_output,
                                            ip_location_with_penalty_and_total_count, v4)

            print(
                f'Our current ip_location_with_penalty_and_total_count length is {len(ip_location_with_penalty_and_total_count)}')

            print(f'Lets save the current result')

            with open(root_dir / 'stats/location_data/ripe_validated_ip_locations_v{}_{}'.format(ip_version, msm_id),
                      'wb') as fp:
                pickle.dump(ip_location_with_penalty_and_total_count, fp, protocol=pickle.HIGHEST_PROTOCOL)

            print(f'Finished saving the results')

        print('Stage 3 : Identifying the big jumps and storing in a dictionary')
        for index, traceroute in enumerate(updated_traceroute_output):

            ripe_hops, actual_ripe_hops, conditional_ripe_hops = get_ripe_hops(traceroute, v4)

            for a_ripe_hop in ripe_hops:
                ip_addresses = a_ripe_hop[0]
                all_latencies = d.get(ip_addresses, [])

                all_min_latencies = a_ripe_hop[1]
                all_max_latencies = a_ripe_hop[2]

                latency_min = round((all_min_latencies[1] - all_min_latencies[0]) / 2, 2)
                all_latencies.append(latency_min)
                d[ip_addresses] = all_latencies

        time = time_end

        print(f'Current count is {count} and dictionary length is {len(d)}')

        # if count % 6 == 0:
        #     print(f'Writing to a file for count {count // 6}')
        #     file_name = f'uniq_ip_dict_{msm_id}_all_links_v{ip_version}_min_all_latencies_only_' + str(count // 6)
        #     save_file = parent_dir / file_name
        #     with open(save_file, 'wb') as fp:
        #         pickle.dump(d, fp, protocol=pickle.HIGHEST_PROTOCOL)

        count += 1

    end_time_int = int(calendar.timegm(end_time.timetuple()))
    end_process_time = datetime.fromtimestamp(end_time_int, tz=timezone.utc).strftime("%m_%d_%Y_%H_%M")

    file_name = f'uniq_ip_dict_{msm_id}_all_links_v{ip_version}_min_all_latencies_only_{end_process_time}_count_' + str(
        count)
    save_file = parent_dir / file_name
    with open(save_file, 'wb') as fp:
        pickle.dump(d, fp, protocol=pickle.HIGHEST_PROTOCOL)
    print(f'Saved the final result to {save_file}')

    return d


if __name__ == '__main__':
    s_time = datetime(2024, 5, 1, 0)
    e_time = datetime(2024, 5, 1, 1)

    # The measurement ids used are 5051 and 5151 for v4 and 6052 and 6152 for v6
    # The number (6) in example below indicates the IP version, which will be according to the measurement IDs
    # That number is mostly used for just saving the results

    result = ripe_process_traceroutes(s_time, e_time, '5051', 4, False)
    print(f'Result length is {len(result)}')
    # result = ripe_process_traceroutes(s_time, e_time, '5151', 4, False)
    # print(f'Result length is {len(result)}')
