import pickle
from pathlib import Path

from tqdm import tqdm
import random

import requests
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed

root_dir = Path(__file__).resolve().parents[2]

# 定义 namedtuple
Location = namedtuple('Location', [
    'city', 'subdivisions', 'country', 'accuracy_radius',
    'latitude', 'longitude', 'autonomous_system_number',
    'network', 'ISP', 'Org'
])


def decode_if_bytes(value):
    if value == '-' or not value:
        return ''
    else:
        return str(value).encode('ascii', 'ignore')


def parse_location(res_data):
    # 尝试提取所有可能的键并转换为 ASCII 编码
    city = decode_if_bytes(res_data.get('city') or res_data.get('cityName') or
                           res_data.get('location', {}).get('city') or
                           res_data.get('data', {}).get('city'))

    subdivisions = decode_if_bytes(res_data.get('region') or res_data.get('regionName') or
                                   res_data.get('stateprov') or res_data.get('state_prov') or
                                   res_data.get('location', {}).get('region', {}).get('name') or
                                   res_data.get('data', {}).get('state_prov'))

    country = decode_if_bytes(res_data.get('country') or res_data.get('countryName') or
                              res_data.get('country_name') or res_data.get('location', {}).get('country', {}).get(
        'name') or
                              res_data.get('data', {}).get('country_name'))

    latitude = decode_if_bytes(res_data.get('latitude') or res_data.get('location', {}).get('latitude') or
                               res_data.get('data', {}).get('latitude'))

    longitude = decode_if_bytes(res_data.get('longitude') or res_data.get('location', {}).get('longitude') or
                                res_data.get('data', {}).get('longitude'))

    autonomous_system_number = decode_if_bytes(res_data.get('asn') or res_data.get('as_no') or
                                               res_data.get('connection', {}).get('asn') or
                                               res_data.get('data', {}).get('asn'))

    network = decode_if_bytes(res_data.get('network') or res_data.get('connection', {}).get('range') or
                              res_data.get('connection', {}).get('route'))

    ISP = decode_if_bytes(res_data.get('isp') or res_data.get('org') or
                          res_data.get('organization') or res_data.get('data', {}).get('isp') or
                          res_data.get('connection', {}).get('isp') or res_data.get('company', {}).get('name'))

    Org = decode_if_bytes(res_data.get('org_name') or res_data.get('organization') or
                          res_data.get('data', {}).get('organization') or
                          res_data.get('connection', {}).get('organization') or res_data.get('company', {}).get('name'))

    location = Location(
        city=city,
        subdivisions=subdivisions,
        country=country,
        accuracy_radius='',  # 这个字段在响应数据中没有，需要根据实际情况处理
        latitude=latitude,
        longitude=longitude,
        autonomous_system_number=autonomous_system_number,
        network=network,
        ISP=ISP,
        Org=Org
    )

    return location


def get_ipgeolocation_response_single(ip, source):
    url = "https://www.iplocation.net/get-ipdata"
    data = {
        "ip": ip,
        "source": source,
        "ipv": "4"
    }

    response = requests.post(url, data=data)
    if response.status_code == 200:
        response_data = response.json()
        location = parse_location(response_data.get('res', {}))
        return source, location
    else:
        response.raise_for_status()


def get_ipgeolocation_response(ip):
    sources = [
        "ip2location", "ipinfo", "dbip", "ipregistry",
        "ipgeolocation", "ipapico", "ipbase", "criminalip"
    ]

    results = []

    with ThreadPoolExecutor(max_workers=len(sources)) as executor:
        future_to_source = {executor.submit(get_ipgeolocation_response_single, ip, source): source for source in
                            sources}

        for future in as_completed(future_to_source):
            source = future_to_source[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                # results.append((source, f"Error: {str(e)}"))
                continue

    # 根据source的顺序对结果进行排序，只保留result[1]
    results = [result[1] for result in sorted(results, key=lambda x: sources.index(x[0]))]
    return results


def load_pre_contents(args):
    tags = args.get('tags', 'default')
    ip_version = args.get('ip_version')

    save_file = f'ipgeolocation_file_v{ip_version}_{tags}'

    save_directory = root_dir / 'stats/location_data/iplocation_files'

    path = Path(save_directory / save_file)

    if path.is_file():
        contents = pickle.load(open(path, 'rb'))
        print(f'Loaded contents from file. Length of contents is {len(contents)}')
        return contents
    else:
        print(f'No file found. Starting afresh')
        return {}


def save_contents_to_file(contents, args):
    tags = args.get('tags', 'default')
    ip_version = args.get('ip_version', 4)

    save_file = f'ipgeolocation_file_v{ip_version}_{tags}'

    save_directory = root_dir / 'stats/location_data/iplocation_files'

    save_directory.mkdir(parents=True, exist_ok=True)

    # print('Saving contents to file')

    with open(save_directory / save_file, 'wb') as fp:
        pickle.dump(contents, fp, protocol=3)


def generate_location_for_list_of_ips(list_of_ips, args=None):

    # Essentially a functionality to continue from where things failed
    contents = load_pre_contents(args)

    try:
        for ip in tqdm(list_of_ips, desc='ipgeo', disable=True):
            if ip in contents.keys():
                continue
            response = get_ipgeolocation_response(ip)
            if not response:
                print(f'Looks like we encountered some error. Lets give up !!')
                break
            else:
                contents[ip] = response
                save_contents_to_file(contents, args)

    except Exception as e:
        print(f'There seems to be some issue, terminating stuff. We got the error {str(e)}')
        exit(1)


def generate_random_ipv4():
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"


if __name__ == '__main__':
    args = {'tags': 'default', 'ip_version': 4}
    # list_of_ips = [generate_random_ipv4() for _ in range(100)]
    list_of_ips = ['66.85.82.9', '156.225.182.1', '67.59.254.241', '103.78.227.1', '193.34.197.140', '23.111.226.1',
                   '193.0.214.1', '152.255.147.235', '216.19.218.1']
    # for ip in tqdm(list_of_ips):
    #     res = get_ipgeolocation_response(ip)
    #     print(res)
    generate_location_for_list_of_ips(list_of_ips, args)
