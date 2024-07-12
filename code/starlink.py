import json
import os
import pickle
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
import requests
import reverse_geocode
from tqdm import tqdm
from collections import namedtuple

root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir))

from code.ip_to_as import whois_itdk_utils, cymru_whois_utils, whois_radb_utils, whois_rpki_utils
from code.location import ripe_geolocation_utils, caida_geolocation_utils, maxmind_utils, ipgeolocation_utils

from code.utils.geolocation_utils import get_country_to_continent_map, get_country_alpha_to_digit
from code.utils import common_utils, merge_data

Location = namedtuple('Location', ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude',
                                   'autonomous_system_number', 'network', 'ISP', 'Org'])
MaxmindLocation = namedtuple('MaxmindLocation',
                             ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude',
                              'autonomous_system_number', 'network'])
LandingPoints = namedtuple('LandingPoints', ['latitude', 'longitude', 'country', 'location', 'cable'])
Cable = namedtuple('Cable', ['name', 'landing_points', 'length', 'owners', 'notes', 'rfs', 'other_info'])


def get_city_coordinates(city_name):
    # 请替换为你的Google Maps API密钥
    API_KEY = 'AIzaSyD202Xzio1yOu5DpC1vV1VlHmOVrZU09nA'
    # 构建请求URL
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={city_name}&key={API_KEY}"
    print(url)
    # 发送请求
    response = requests.get(url)
    # 解析响应数据
    if response.status_code == 200:
        data = response.json()
        # 检查是否有结果
        if data['results']:
            # 获取第一个结果的坐标
            location = data['results'][0]['geometry']['location']
            return (location['lat'], location['lng'])
        else:
            return "No results found for the given city."
    else:
        return "Error in the request."


def get_all_pops_coordinates(path):
    df = pd.read_csv(path)
    df['city_coords'] = df['city'].apply(get_city_coordinates)
    df.to_csv(path, index=False)


def get_all_pops_ip(pop_info_path, link_path):
    links_starlink2_df = pd.read_csv(link_path)
    pops_info_df = pd.read_csv(pop_info_path)
    # pop_id,city,country,pop_name,city_coords,
    pops_info_df = pops_info_df[['pop_id', 'city', 'country', 'pop_name', 'city_coords']].drop_duplicates()
    # get id to ip mapping
    id_ip_list = []
    for index, row in links_starlink2_df.iterrows():
        id_ip_list.append({'pop_id': row['src_id'], 'ip': row['src_ip']})
        id_ip_list.append({'pop_id': row['dst_id'], 'ip': row['dst_ip']})
    id_ip_df = pd.DataFrame(id_ip_list)

    # Merge with pops_info_df
    results_df = pd.merge(pops_info_df, id_ip_df, on='pop_id', how='outer')
    results_df.to_csv(pop_info_path, index=False)
    print(results_df)


def process_links_txt_to_pickle_links_ips(inpath, links_path, ip_path):
    with open(inpath, 'r') as f:
        data = f.readlines()

    links = set()
    ips = set()

    for line in data:
        line = line.strip()
        ip_pair = line.split('-')
        # to tuple
        link = (ip_pair[0], ip_pair[1])
        links.add(link)
        ips.update(set(ip_pair))

    with open(links_path, 'wb') as f:
        pickle.dump(links, f)

    with open(ip_path, 'wb') as f:
        pickle.dump(ips, f)


def process_links_csv_to_pickle_links_ips(inpath, links_path, ip_path):
    data = pd.read_csv(inpath)
    links = set()
    ips = set()
    for index, row in data.iterrows():
        link = (row['src_ip'], row['dst_ip'])
        links.add(link)
        ips.add(row['src_ip'])
        ips.add(row['dst_ip'])

    with open(links_path, 'wb') as f:
        pickle.dump(links, f)

    with open(ip_path, 'wb') as f:
        pickle.dump(ips, f)


def process_ip_info_to_cluster_file(in_path, latlon_cluster_path, country_cluster_path, continent_cluster_path):
    data = pd.read_csv(in_path)
    data = data[['pop_id', 'ip', 'city_coords']]
    data = data.dropna()
    data['city_coords'] = data['city_coords'].apply(lambda x: eval(x) if pd.notnull(x) else None)
    data = data.values.tolist()
    # print(data[:10])

    # 初始化结果字典
    geolocation_latlon_cluster_and_score_map = {}
    geolocation_country_cluster = {}
    geolocation_continent_cluster = {}

    # 获取国家和大洲映射表
    country_to_continent_map = get_country_to_continent_map()
    country_2alpha_to_digit = get_country_alpha_to_digit()

    # 反向地理编码
    def reverse_geocode_coords(coords):
        result = reverse_geocode.search([coords])[0]
        country_code = country_2alpha_to_digit[result['country_code']]
        continent_code = country_to_continent_map[country_code]
        return country_code, continent_code

    # 生成地理位置聚类
    for pop_id, ip, coords in data:
        geolocation_latlon_cluster_and_score_map[ip] = ([[coords]], [1.0], 0)
        country_code, continent_code = reverse_geocode_coords(coords)
        geolocation_country_cluster[ip] = ([country_code], [1.0])
        geolocation_continent_cluster[ip] = ([continent_code], [1.0])

    # 保存结果
    with open(latlon_cluster_path, 'wb') as f:
        pickle.dump(geolocation_latlon_cluster_and_score_map, f)

    with open(country_cluster_path, 'wb') as f:
        pickle.dump(geolocation_country_cluster, f)

    with open(continent_cluster_path, 'wb') as f:
        pickle.dump(geolocation_continent_cluster, f)

    print("Results saved successfully.")


def read_pickle(file_path, start=0, end=0):
    with open(file_path, 'rb') as f:
        data = pickle.load(f)
    data = dict(data)
    # convert keys to str
    data = {str(k): v for k, v in data.items()}
    print(json.dumps(data, indent=4))
    with open(file_path.with_suffix('.json'), 'w') as f:
        json.dump(data, f, indent=4)


def generate_geolocation(ips_list, ip_version, args):
    print('1. RIPE geolocation')
    ripe_location = ripe_geolocation_utils.generate_location_for_list_of_ips_ripe(ips_list, ip_version)
    print(f'RIPE: Results for {len(ripe_location)} IPs')
    print('2. CAIDA geolocation')
    caida_location = caida_geolocation_utils.generate_location_for_list_of_ips(ips_list)
    print(f'CAIDA: Results for {len(caida_location)} IPs')
    print('3. Maxmind geolocation')
    maxmind_location, skipped_ips = maxmind_utils.generate_locations_for_list_of_ips(ips_list, ip_version)
    print(f'Maxmind: Results for {len(maxmind_location)} IPs')
    print('4. IPGeolocation geolocation')
    ipgeolocation_utils.generate_location_for_list_of_ips(ips_list, args=args)
    print(datetime.now(), '******* Merging the geolocation results *******')
    merge_data.common_merge_operation(root_dir / 'stats/location_data/iplocation_files', 2, [], ['ipgeolocation_file_'],
                                      True,
                                      f'iplocation_location_output_v{ip_version}_default')


def generate_ip_to_as_mapping(ip_version, list_of_ips, args, in_chunks):
    print('1. CAIDA ITDK queries')
    caida_output = whois_itdk_utils.generate_ip2as_for_list_of_ips(ip_version, list_of_ips, 'default')
    print(f'CAIDA: Results for {len(caida_output)} IPs')
    print('2. Cymru queries')
    cymru_output = cymru_whois_utils.generate_ip2as_for_list_of_ips(list_of_ips, ip_version, 'default')
    print(f'Cymru: Results for {len(cymru_output)} IPs')
    print('3. RADB queries')
    args['whois_cmd_location'] = '/home/lintao/anaconda3/envs/ki3/bin/whois'
    radb_output = whois_radb_utils.generate_ip2as_for_list_of_ips(ip_version, list_of_ips, args=args,
                                                                  in_chunks=in_chunks)
    print(f'RADB: Results for {len(radb_output)} IPs')
    print('4. RPKI queries')
    rpki_output = whois_rpki_utils.generate_ip2as_for_list_of_ips(ip_version, list_of_ips, args=args,
                                                                  in_chunks=in_chunks)
    print(f'RPKI: Results for {len(rpki_output)} IPs')


if __name__ == '__main__':
    # # get_all_pops_coordinates(root_dir / 'stats/pops_info.csv')
    # get_all_pops_ip(root_dir / 'stats/pops_info.csv', root_dir / 'stats/links_starlink3.csv')
    #
    # ip_version = 4
    # args = {'chromedriver_location': root_dir / 'chromedriver', 'ip_version': ip_version, 'tags': 'default'}
    #
    # print(datetime.now(), '******* Generating all the unique IPs and links *******')
    # process_links_csv_to_pickle_links_ips(root_dir / 'stats/links_starlink3.csv',
    #                                       root_dir / 'stats/mapping_outputs/links_v4',
    #                                       root_dir / 'stats/mapping_outputs/all_ips_v4')
    #
    # print(datetime.now(), '******* Reading the IPs list *******')
    # with open(root_dir / f'stats/mapping_outputs/all_ips_v{ip_version}', 'rb') as fp:
    #     ips_list = pickle.load(fp)
    #
    # # print(datetime.now(), '******* Generating geolocation results *******')
    # # mode = 0
    # # generate_geolocation(ips_list, ip_version, args)
    #
    # print(datetime.now(), '******* Generating geolocation results from pop info*******')
    # mode = 1
    # process_ip_info_to_cluster_file(root_dir / 'stats/pops_info.csv',
    #                                 root_dir / 'stats/mapping_outputs/geolocation_latlon_cluster_and_score_map_sol_validated_v4',
    #                                 root_dir / 'stats/mapping_outputs/geolocation_country_cluster_sol_validated_v4',
    #                                 root_dir / 'stats/mapping_outputs/geolocation_continent_cluster_sol_validated_v4')
    #
    # print(datetime.now(), '******* IP to AS mapping *******')
    # generate_ip_to_as_mapping(ip_version, ips_list, args, False)
    #
    # print(datetime.now(), '******* Nautilus Mapping *******')
    # print("Generate an initial mapping for each category")
    # common_utils.generate_cable_mapping(mode=mode, ip_version=ip_version, sol_threshold=0.05)
    # print("Mearge mapping results of multiple experiments for each category")
    # merge_data.common_merge_operation(root_dir / 'stats/mapping_outputs', 1, [], ['v4'], True, None)
    # print("Merging the results for all categories")
    # common_utils.generate_final_mapping(mode=mode, ip_version=ip_version, threshold=0.05)
    # print("Re-updating the categories map")
    # common_utils.regenerate_categories_map(mode=mode, ip_version=ip_version)
    #
    # # read_pickle(root_dir / 'stats/mapping_outputs/link_to_cable_and_score_mapping_v4')
    # read_pickle(root_dir / 'stats/mapping_outputs/link_to_cable_and_score_mapping_sol_validated_v4')
    # # read_pickle(root_dir / 'stats/mapping_outputs/geolocation_latlon_cluster_and_score_map_sol_validated_v4', 0, 10)
    # # read_pickle(root_dir / 'stats/mapping_outputs/geolocation_continent_cluster_sol_validated_v4', 0, 10)
    # # read_pickle(root_dir / 'stats/mapping_outputs/geolocation_country_cluster_sol_validated_v4', 0, 10)

    print(get_city_coordinates('Beijing'))
    print(get_city_coordinates('Sanya'))