import pickle
from pathlib import Path
import subprocess

import os, sys

root_dir = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(root_dir))

from code.utils.merge_data import save_results_to_file
from collections import namedtuple
from code.utils.geolocation_utils import generate_latlon_cluster_and_score_map, generate_categories, Location, \
    MaxmindLocation
from code.utils.as_utils import generate_closest_submarine_org
from code.utils.traceroute_utils import load_all_links_and_ips_data, generate_test_case_links_and_ips_data

from code.submarine.telegeography_submarine import find_intersecting_cables, Cable, LandingPoints, \
    get_all_latlon_locations_ball_tree, get_cable_by_cable_id
import math
import numpy as np
from itertools import product
from haversine import haversine, Unit

Cable = namedtuple('Cable', ['name', 'landing_points', 'length', 'owners', 'notes', 'rfs', 'other_info'])


def get_cable_details():
    save_file = root_dir / 'stats/submarine_data/cable_info_dict'

    if Path(save_file).exists():
        with open(save_file, 'rb') as fp:
            cable_dict = pickle.load(fp)
        return cable_dict
    else:
        print(f'Run the submarine module to generate necessary data')
        sys.exit(1)


def get_future_cables(cable_dict):
    return [cable for cable, values in cable_dict.items() if values.rfs >= 2022]


def get_submarine_owners():
    save_file = root_dir / 'stats/submarine_data/owners_dict'

    if Path(save_file).exists():
        with open(save_file, 'rb') as fp:
            submarine_owners_dict = pickle.load(fp)
        return submarine_owners_dict
    else:
        print(f'Run the submarine module to generate necessary data')
        sys.exit(1)


def load_required_files(all_ips, links, mode=2, ip_version=4, sol_threshold=0.01, geolocation_threshold=0.6,
                        ignore=True, suffix='default'):
    # Ideally we want to load things straight away in which case we don't need all_ips and links
    geolocation_latlon_cluster_and_score_map, geolocation_latlon_cluster_and_score_map_sol_validated = generate_latlon_cluster_and_score_map(
        all_ips, ip_version=ip_version, mode=mode, threshold=sol_threshold, suffix=suffix)

    categories_map, categories_map_sol_validated = generate_categories(all_ips, links,
                                                                       geolocation_latlon_cluster_and_score_map,
                                                                       geolocation_latlon_cluster_and_score_map_sol_validated,
                                                                       ip_version=ip_version, mode=mode,
                                                                       sol_threshold=sol_threshold,
                                                                       geolocation_threshold=geolocation_threshold,
                                                                       ignore=ignore, suffix=suffix)

    return geolocation_latlon_cluster_and_score_map, geolocation_latlon_cluster_and_score_map_sol_validated, categories_map, categories_map_sol_validated


def get_sorted_mean_clusters(cluster):
    return [list(item) for item in list(map(lambda p: np.mean(p, axis=0), sorted(cluster, key=len, reverse=True)))]


def return_mean_and_len_clusters(ip_address, geolocation_latlon_cluster_and_scores_map):
    latlon_cluster, len_cluster, _ = geolocation_latlon_cluster_and_scores_map[ip_address]
    return (get_sorted_mean_clusters(latlon_cluster), sorted(len_cluster, reverse=True))


def convert_degrees_to_randians(item):
    return tuple(map(math.radians, item))


def list_conversion_from_array(array):
    return [i.tolist() for i in array]


def get_landing_point_info(tree_index, landing_points_dict, latlon_dict, latlons):
    return landing_points_dict[latlon_dict[latlons[tree_index]]]


def get_cable_for_given_latlon_pair(latlon_pair, tree, scores_pair, future_cables, landing_points_dict, latlon_dict,
                                    latlons, category):
    radians_latlon_pair = list(map(convert_degrees_to_randians, latlon_pair))
    out = {}
    radius_increase = 50
    if 'te' in category:
        current_radius = 500
    else:
        current_radius = 1000
    match_count = 0
    while match_count < 2:
        ind, dist = tuple(map(list_conversion_from_array,
                              tree.query_radius(radians_latlon_pair, current_radius / 6371, return_distance=True,
                                                sort_results=True)))
        for index_pairs, dist_pairs in zip(product(ind[0], ind[1]), product(dist[0], dist[1])):
            # 遍历这些对，如果索引不相同，获取对应的登陆点信息。
            if index_pairs[0] != index_pairs[1]:
                landing_point_1, landing_point_2 = get_landing_point_info(index_pairs[0], landing_points_dict,
                                                                          latlon_dict, latlons), get_landing_point_info(
                    index_pairs[1], landing_points_dict, latlon_dict, latlons)
                # 找到两个登陆点的交集海缆。不会判断连接关系，只要是同一条海缆上的两个登陆站就默认为相连
                cables = find_intersecting_cables(landing_point_1.cable, landing_point_2.cable)
                cables = [cable for cable in cables if cable not in future_cables]
                if len(cables) > 0:
                    for cable in cables:
                        identified_cable = get_cable_by_cable_id(cable).name
                        scores_val = out.get(identified_cable, [])
                        scores_val.append((latlon_pair, (landing_point_1, landing_point_2), scores_pair, dist_pairs))
                        out[identified_cable] = scores_val
                    match_count += 1
        current_radius += radius_increase
        if current_radius >= 1000:
            break

    return out


def update_dict(dict_1, dict_2):
    out_dict = dict_1.copy()
    for k, v in dict_2.items():
        res = out_dict.get(k, [])
        res.extend(v)
        out_dict[k] = res
    return out_dict


def update_score_tuple(list_of_tuples_from_geolocation, owner_score_tuple):
    final_score_tuple = []
    for geolocation_tuple in list_of_tuples_from_geolocation:
        geolocation_tuple_list = list(geolocation_tuple)
        geolocation_tuple_list.append(owner_score_tuple)
        single_score_tuple = tuple(geolocation_tuple_list)
        final_score_tuple.append(single_score_tuple)
    return final_score_tuple


def generate_cable_mapping_for_given_category(category_links, latlon_cluster_and_score_map, category, tree,
                                              future_cables, closest_submarine_org, submarine_owners_dict, cable_dict,
                                              landing_points_dict, latlon_dict, latlons, save_file=None, suffix='default'):
    save_directory = root_dir / f'stats/mapping_outputs_{suffix}'
    save_directory.mkdir(parents=True, exist_ok=True)

    cable_mapping = {}

    for count, (ip_1, ip_2) in enumerate(category_links):

        link_all_scores_cable_map = {}

        mean_cluster_1, len_cluster_1 = return_mean_and_len_clusters(ip_1, latlon_cluster_and_score_map)
        mean_cluster_2, len_cluster_2 = return_mean_and_len_clusters(ip_2, latlon_cluster_and_score_map)

        scores_cable_map = {}

        for mean_cluster_combination, scores_combination in zip(product(mean_cluster_1, mean_cluster_2),
                                                                product(len_cluster_1, len_cluster_2)):
            cables = get_cable_for_given_latlon_pair(mean_cluster_combination, tree, scores_combination, future_cables,
                                                     landing_points_dict, latlon_dict, latlons, category)
            scores_cable_map = update_dict(scores_cable_map, cables)

        org_1 = closest_submarine_org.get(ip_1, None)
        org_2 = closest_submarine_org.get(ip_2, None)

        org_1_cables_names, org_2_cables_names = [], []

        if org_1 or org_2:
            org_1_cables, org_2_cables = [], []
            if org_1:
                for org in org_1:
                    org_1_cables.extend(submarine_owners_dict[org])
                for cable in org_1_cables:
                    org_1_cables_names.append(cable_dict[cable].name)

            if org_2:
                for org in org_2:
                    org_2_cables.extend(submarine_owners_dict[org])
                for cable in org_2_cables:
                    org_2_cables_names.append(cable_dict[cable].name)

        if len(scores_cable_map) > 0:
            for cable, geolocation_tuples in scores_cable_map.items():
                owner_score_tuple = []
                if cable in org_1_cables_names:
                    owner_score_tuple.append(1)
                else:
                    owner_score_tuple.append(0)

                if cable in org_2_cables_names:
                    owner_score_tuple.append(1)
                else:
                    owner_score_tuple.append(0)

                link_all_scores_cable_map[cable] = update_score_tuple(geolocation_tuples, tuple(owner_score_tuple))

        cable_mapping[(ip_1, ip_2)] = link_all_scores_cable_map

        if count % 500 == 0:
            print(f'Finished {count} of {len(category_links)}')

    save_results_to_file(cable_mapping, str(save_directory), save_file)

    return cable_mapping


def general_cable_mapping_helper(categories_map, latlon_cluster_and_score_map, tree, future_cables,
                                 closest_submarine_org, submarine_owners_dict, cable_dict, landing_points_dict,
                                 latlon_dict, latlons, max_links_to_process=None, server_id=None, mode=0, ip_version=4, suffix='default'):
    cable_mapping_all_categories = {}

    for category in categories_map:
        if category != 'de_te':
            if server_id:
                if server_id * max_links_to_process[category] > len(categories_map[category]):
                    category_links = categories_map[category][(server_id - 1) * max_links_to_process[category]:]
                else:
                    category_links = categories_map[category][
                                     (server_id - 1) * max_links_to_process[category]: server_id * max_links_to_process[
                                         category]]
            else:
                category_links = categories_map[category]

            print(f'For category {category}, we are processing {len(category_links)} links')

            if mode == 0:
                save_file = 'cable_mapping_{}_v{}'.format(category, ip_version)
                if server_id:
                    save_file += '_s{}'.format(server_id)

                cable_mapping = generate_cable_mapping_for_given_category(category_links, latlon_cluster_and_score_map,
                                                                          category, tree, future_cables,
                                                                          closest_submarine_org, submarine_owners_dict,
                                                                          cable_dict, landing_points_dict, latlon_dict,
                                                                          latlons, save_file, suffix)

            else:
                save_file = 'cable_mapping_sol_validated_{}_v{}'.format(category, ip_version)
                if server_id:
                    save_file += '_s{}'.format(server_id)

                cable_mapping = generate_cable_mapping_for_given_category(category_links, latlon_cluster_and_score_map,
                                                                          category, tree, future_cables,
                                                                          closest_submarine_org, submarine_owners_dict,
                                                                          cable_dict, landing_points_dict, latlon_dict,
                                                                          latlons, save_file, suffix)

            cable_mapping_all_categories[category] = cable_mapping

    return cable_mapping_all_categories


def generate_cable_mapping(max_links_to_process=None, max_links_to_process_sol_validated=None, server_id=None, mode=2,
                           ip_version=4, sol_threshold=0.01, geolocation_threshold=0.6, ignore=True, suffix='default'):
    """
    This function generates the cable mapping for all the categories
    :param mode: The mode to process the links, 0 - Only geolocation, 1 - SoL validated geolocation, 2 - Generate both results
    """
    links, all_ips = load_all_links_and_ips_data(ip_version=ip_version)
    geolocation_latlon_cluster_and_score_map, geolocation_latlon_cluster_and_score_map_sol_validated, categories_map, categories_map_sol_validated = load_required_files(
        all_ips, links, mode=mode, ip_version=ip_version, sol_threshold=sol_threshold,
        geolocation_threshold=geolocation_threshold, ignore=ignore, suffix=suffix)

    cable_dict = get_cable_details()
    future_cables = get_future_cables(cable_dict)
    submarine_owners_dict = get_submarine_owners()
    landing_points_dict, latlon_dict, latlons, tree = get_all_latlon_locations_ball_tree()

    closest_submarine_org = generate_closest_submarine_org(all_ips, ip_version=ip_version, suffix=suffix)

    cable_mapping, cable_mapping_sol_validated = {}, {}

    if mode in [0, 2]:
        print(f'Currently processing mode : {mode}')
        cable_mapping = general_cable_mapping_helper(categories_map, geolocation_latlon_cluster_and_score_map, tree,
                                                     future_cables, closest_submarine_org, submarine_owners_dict,
                                                     cable_dict,
                                                     landing_points_dict, latlon_dict, latlons,
                                                     max_links_to_process=max_links_to_process, server_id=server_id,
                                                     mode=0, ip_version=ip_version, suffix=suffix)

    if mode in [1, 2]:
        print(f'Currently processing mode : {mode}')
        cable_mapping_sol_validated = general_cable_mapping_helper(categories_map_sol_validated,
                                                                   geolocation_latlon_cluster_and_score_map_sol_validated,
                                                                   tree, future_cables, closest_submarine_org,
                                                                   submarine_owners_dict, cable_dict,
                                                                   landing_points_dict, latlon_dict, latlons,
                                                                   max_links_to_process=max_links_to_process_sol_validated,
                                                                   server_id=server_id,
                                                                   mode=1, ip_version=ip_version, suffix=suffix)

    return cable_mapping, cable_mapping_sol_validated


def generate_cable_mapping_test(mode=2, ip_version=4, sol_threshold=0.01, geolocation_threshold=0.6, ignore=True,
                                max_links_to_process=None, max_links_to_process_sol_validated=None, server_id=None, suffix='default'):
    links, all_ips = generate_test_case_links_and_ips_data(ip_version=ip_version)
    geolocation_latlon_cluster_and_score_map, geolocation_latlon_cluster_and_score_map_sol_validated, categories_map, categories_map_sol_validated = load_required_files(
        all_ips, links, mode=mode, ip_version=ip_version, sol_threshold=sol_threshold,
        geolocation_threshold=geolocation_threshold, ignore=ignore, suffix=suffix)

    cable_dict = get_cable_details()
    future_cables = get_future_cables(cable_dict)
    submarine_owners_dict = get_submarine_owners()
    landing_points_dict, latlon_dict, latlons, tree = get_all_latlon_locations_ball_tree()

    closest_submarine_org = generate_closest_submarine_org(all_ips, ip_version=ip_version, suffix=suffix)

    cable_mapping, cable_mapping_sol_validated = {}, {}

    if mode in [0, 2]:
        print(f'Currently processing mode : {mode}')
        cable_mapping = general_cable_mapping_helper(categories_map, geolocation_latlon_cluster_and_score_map, tree,
                                                     future_cables, closest_submarine_org, submarine_owners_dict,
                                                     cable_dict,
                                                     landing_points_dict, latlon_dict, latlons,
                                                     max_links_to_process=max_links_to_process, server_id=server_id,
                                                     mode=0, ip_version=ip_version)

    if mode in [1, 2]:
        print(f'Currently processing mode : {mode}')
        cable_mapping_sol_validated = general_cable_mapping_helper(categories_map_sol_validated,
                                                                   geolocation_latlon_cluster_and_score_map_sol_validated,
                                                                   tree, future_cables, closest_submarine_org,
                                                                   submarine_owners_dict, cable_dict,
                                                                   landing_points_dict, latlon_dict, latlons,
                                                                   max_links_to_process=max_links_to_process_sol_validated,
                                                                   server_id=server_id,
                                                                   mode=1, ip_version=ip_version)

    return cable_mapping, cable_mapping_sol_validated


def get_load_all_cable_mapping_merged_output(mode=2, ip_version=4, suffix='default'):
    save_directory = root_dir / f'stats/mapping_outputs_{suffix}'

    categories = ['bg_oc', 'og_oc', 'bb_oc', 'bg_te', 'og_te', 'bb_te']

    cable_mapping, cable_mapping_sol_validated = {}, {}

    cable_mapping_files = ['cable_mapping_{}_v{}_merged'.format(category, ip_version) for category in categories]
    cable_mapping_sol_validated_files = ['cable_mapping_sol_validated_{}_v{}_merged'.format(category, ip_version) for
                                         category in categories]

    mapping = {}
    mapping_sol_validated = {}

    if mode in [0, 2]:
        for index, file in enumerate(cable_mapping_files):
            with open(save_directory / file, 'rb') as fp:
                content = pickle.load(fp)
            mapping[categories[index]] = content

            print(f'Only geolocation: Finished loading for {categories[index]}')

            del (content)

    if mode in [1, 2]:
        for index, file in enumerate(cable_mapping_sol_validated_files):
            with open(save_directory / file, 'rb') as fp:
                content = pickle.load(fp)
            mapping_sol_validated[categories[index]] = content

            print(f'SoL validated: Finished loading for {categories[index]}')

            del (content)

    return mapping, mapping_sol_validated


def generate_reverse_landing_points_dict():
    landing_points_dict, latlon_dict, latlons, tree = get_all_latlon_locations_ball_tree()
    return {v._replace(cable=tuple(v.cable)): k for k, v in landing_points_dict.items()}


# Temporarily placing the loading cable to lp ids function here, will have to move this to the proper module
def load_cable_to_lp_ids():
    with open('stats/submarine_data/cable_to_connected_location_ids', 'rb') as fp:
        cable_to_lp_ids = pickle.load(fp)

    return cable_to_lp_ids


def get_landing_point_id_from_landing_points_list(landing_points_list, reverse_landing_points_dict):
    landing_points_ids = []
    for landing_point in landing_points_list:
        landing_points_id = reverse_landing_points_dict.get(landing_point._replace(cable=tuple(landing_point.cable)),
                                                            '')
        landing_points_ids.append(landing_points_id)
    return tuple(landing_points_ids)


def assign_overall_score(score_tuple, weight_tuple, category):
    if 'te' in category:
        scale_factor = 0.5
    else:
        scale_factor = 1

    constant_factor = 0.5

    # score tuple
    # 	0 -> IP geolocation
    #	1 -> Landing points details
    #	2 -> geolocation score (geolocation clustering score)
    #	3 -> distance from IP geolocation to identified landing point
    #	4 -> as owner scores

    geolocation_score = sum(score_tuple[2]) * weight_tuple[0]

    # Distance score should be ideally 0, so that we get reverse distance score of 2
    # Worse case we get distance score of 2, which implies both the points are 1000 km away
    distance_score = sum(score_tuple[3]) / (1000 / 6371)
    reverse_distance_score = (2 - distance_score) * weight_tuple[1]

    as_owner_score = sum(score_tuple[4]) * weight_tuple[2]

    # Check to help with re-classification of link to definite terrestrial
    # If the landing points are way to far (ie., 2x times the actual distance between the IPs)
    if 2 * haversine(score_tuple[0][0], score_tuple[0][1], unit=Unit.RADIANS) < sum(score_tuple[3]):
        return None
    else:
        return (
            score_tuple[1],
            (geolocation_score + reverse_distance_score + as_owner_score) * constant_factor * scale_factor)


def select_cables_for_given_link(link, scores_and_cables, weight_tuple, de_te_additions, cable_to_lp_ids, category,
                                 reverse_landing_points_dict, threshold=0.05):
    """检查映射出的登陆站是否相连"""
    ret_dict = {}

    # de_te_additions to take note of all links that are getting re-classified as definite terrestrial

    # This means we haven't been able to map to any cable
    if len(scores_and_cables) == 0:
        return {}, 0

    # Let's examine all the predicted cables
    for cable, score_tuples in scores_and_cables.items():
        scores = []
        for score_tuple in score_tuples:
            score_for_tuple = assign_overall_score(score_tuple, weight_tuple, category)
            res = True
            if score_for_tuple:
                lp_id = get_landing_point_id_from_landing_points_list(score_for_tuple[0], reverse_landing_points_dict)
                try:
                    cable_connected_points = cable_to_lp_ids[cable]
                    # Check to see if both the landing points are connected
                    res = any(len(set(lp_id) & set(item)) == 2 for item in cable_connected_points)
                except:
                    pass
                if res:
                    score_for_tuple = (lp_id, score_for_tuple[-1])
                    scores.append(score_for_tuple)

        if len(scores) > 0:
            max_score = max(scores, key=lambda x: x[1])
            # Checking if other selections are within the threshold
            ret_cable_scores = [item for item in sorted(scores, key=lambda x: x[1], reverse=True) if
                                (max_score[1] - item[1]) <= max_score[1] * threshold]
            ret_dict[cable] = ret_cable_scores

    if len(ret_dict) > 0:
        return {k: v for k, v in sorted(ret_dict.items(), key=lambda item: item[1][0][1], reverse=True)}, 0
    else:
        if res == True:
            de_te_additions.append(link)
            return {}, 1
        return {}, 0


def generate_final_mapping_helper(cable_mapping, de_te_additions, cable_to_lp_ids, reverse_landing_points_dict,
                                  threshold=0.05):
    link_to_cable_and_score_mapping = {}

    for category, category_cable_mapping in cable_mapping.items():
        print(f'Currenlty processing {category}')
        for count, (link, scores_and_cables) in enumerate(category_cable_mapping.items()):
            # Getting the scores dict
            cables, de_te_added = select_cables_for_given_link(link, scores_and_cables, (0.5, 0.4, 0.1),
                                                               de_te_additions, cable_to_lp_ids, category,
                                                               reverse_landing_points_dict, threshold=threshold)

            if len(cables) > 0:
                # Earlier we selected all cables where each landing point was within 0.05 of that particular cable's max value
                # Now we prune based on overall max values
                all_max_scores = [content[0][-1] for content in cables.values()]
                overall_max_score = max(all_max_scores)
                all_max_scores_above_threshold = [score for score in all_max_scores if
                                                  (overall_max_score - score) <= overall_max_score * threshold]

                selected_cables = []
                all_cables = list(cables.keys())

                for idx, cnt in enumerate(all_max_scores_above_threshold):
                    cable_name = all_cables[idx]
                    selected_cables.append(cable_name)

                score = all_max_scores_above_threshold

                selected_landing_points = []
                for cable in selected_cables:
                    contents = cables[cable]
                    single_cable_landing_points = []
                    # Just re-examining again the selected landing points per cable based on the overall max score
                    for single_content in contents:
                        c_score = single_content[-1]
                        if (overall_max_score - c_score) <= overall_max_score * threshold:
                            landing_points = single_content[0]
                            single_cable_landing_points.append(landing_points)
                    landing_points = list(set(landing_points))
                    selected_landing_points.append(single_cable_landing_points)

            else:
                selected_cables = ''
                score = 0
                selected_landing_points = ''

            # Let's generate the final scores file only if it satisfied the additional de_te constraints
            if de_te_added == 0:
                link_to_cable_and_score_mapping[link] = (
                    len(cables), selected_cables, score, selected_landing_points, category)

            if count % 100000 == 0:
                print(f'Finised processing {count} of {len(category_cable_mapping)} links')

    return link_to_cable_and_score_mapping


def generate_final_mapping(mode=2, ip_version=4, threshold=0.05, suffix='default'):
    save_directory = root_dir / f'stats/mapping_outputs_{suffix}'

    # Let's first load all the merged output for each category
    cable_mapping, cable_mapping_sol_validated = get_load_all_cable_mapping_merged_output(mode=mode,
                                                                                          ip_version=ip_version, suffix=suffix)

    # Loading the cable to connected landing points dict
    cable_to_lp_ids = load_cable_to_lp_ids()

    reverse_landing_points_dict = generate_reverse_landing_points_dict()

    de_te_additions, de_te_additions_sol_validated = [], []

    link_to_cable_and_score_mapping, link_to_cable_and_score_mapping_sol_validated = {}, {}

    if mode in [0, 2]:
        link_to_cable_and_score_mapping = generate_final_mapping_helper(cable_mapping, de_te_additions, cable_to_lp_ids,
                                                                        reverse_landing_points_dict,
                                                                        threshold=threshold)
        save_results_to_file(link_to_cable_and_score_mapping, str(save_directory),
                             'link_to_cable_and_score_mapping_v{}'.format(ip_version))
        save_results_to_file(de_te_additions, str(save_directory), 'additional_de_te_links_v{}'.format(ip_version))

        del (link_to_cable_and_score_mapping)
        del (de_te_additions)

    if mode in [1, 2]:
        link_to_cable_and_score_mapping_sol_validated = generate_final_mapping_helper(cable_mapping_sol_validated,
                                                                                      de_te_additions_sol_validated,
                                                                                      cable_to_lp_ids,
                                                                                      reverse_landing_points_dict,
                                                                                      threshold=threshold)
        save_results_to_file(link_to_cable_and_score_mapping_sol_validated, str(save_directory),
                             'link_to_cable_and_score_mapping_sol_validated_v{}'.format(ip_version))
        save_results_to_file(de_te_additions_sol_validated, str(save_directory),
                             'additional_de_te_links_sol_validated_v{}'.format(ip_version))

        del (link_to_cable_and_score_mapping_sol_validated)
        del (de_te_additions_sol_validated)


# return link_to_cable_and_score_mapping, de_te_additions, link_to_cable_and_score_mapping_sol_validated, de_te_additions_sol_validated


def generate_final_mapping_test(cable_mapping, cable_mapping_sol_validated, mode=2, ip_version=4, threshold=0.05, suffix='default'):
    save_directory = root_dir / f'stats/mapping_outputs_{suffix}'

    # Loading the cable to connected landing points dict
    cable_to_lp_ids = load_cable_to_lp_ids()

    reverse_landing_points_dict = generate_reverse_landing_points_dict()

    de_te_additions, de_te_additions_sol_validated = [], []

    link_to_cable_and_score_mapping, link_to_cable_and_score_mapping_sol_validated = {}, {}

    if mode in [0, 2]:
        link_to_cable_and_score_mapping = generate_final_mapping_helper(cable_mapping, de_te_additions, cable_to_lp_ids,
                                                                        reverse_landing_points_dict,
                                                                        threshold=threshold)
        save_results_to_file(link_to_cable_and_score_mapping, str(save_directory),
                             'link_to_cable_and_score_mapping_v{}'.format(ip_version))
        save_results_to_file(de_te_additions, str(save_directory), 'additional_de_te_links_v{}'.format(ip_version))

        del (link_to_cable_and_score_mapping)
        del (de_te_additions)

    if mode in [1, 2]:
        link_to_cable_and_score_mapping_sol_validated = generate_final_mapping_helper(cable_mapping_sol_validated,
                                                                                      de_te_additions_sol_validated,
                                                                                      cable_to_lp_ids,
                                                                                      reverse_landing_points_dict,
                                                                                      threshold=threshold)
        save_results_to_file(link_to_cable_and_score_mapping_sol_validated, str(save_directory),
                             'link_to_cable_and_score_mapping_sol_validated_v{}'.format(ip_version))
        save_results_to_file(de_te_additions_sol_validated, str(save_directory),
                             'additional_de_te_links_sol_validated_v{}'.format(ip_version))

        del (link_to_cable_and_score_mapping_sol_validated)
        del (de_te_additions_sol_validated)


def regenerate_categories_map_helper(categories_map, de_te_additions, ip_version=4):
    new_categories_map = {'bg_oc': [], 'og_oc': [], 'bb_oc': [],
                          'bg_te': [], 'og_te': [], 'bb_te': [], 'de_te': []}

    for key in new_categories_map:
        if key != 'de_te':
            local_var = set(categories_map[key]).difference(set(de_te_additions))
            new_categories_map[key] = list(local_var)
            print(f'For category {key}, earlier {len(categories_map[key])}, it is now {len(new_categories_map[key])}')
        else:
            local_var = categories_map[key].copy()
            local_var.extend(de_te_additions)
            new_categories_map[key] = list(set(local_var))
            print(f'For category {key}, earlier {len(categories_map[key])}, it is now {len(new_categories_map[key])}')

    return new_categories_map


def regenerate_categories_map(mode=2, ip_version=4, suffix='default'):
    save_directory = root_dir / f'stats/mapping_outputs_{suffix}'

    if mode in [0, 2]:
        with open(save_directory / 'categories_map_v{}'.format(ip_version), 'rb') as fp:
            categories_map = pickle.load(fp)

        with open(save_directory / 'additional_de_te_links_v{}'.format(ip_version), 'rb') as fp:
            de_te_additions = pickle.load(fp)

        new_categories_map = regenerate_categories_map_helper(categories_map, de_te_additions, ip_version=ip_version)
        save_results_to_file(new_categories_map, str(save_directory), 'categories_map_updated_v{}'.format(ip_version))

    if mode in [1, 2]:
        with open(save_directory / 'categories_map_sol_validated_v{}'.format(ip_version), 'rb') as fp:
            categories_map_sol_validated = pickle.load(fp)

        with open(save_directory / 'additional_de_te_links_sol_validated_v{}'.format(ip_version), 'rb') as fp:
            de_te_additions_sol_validated = pickle.load(fp)

        new_categories_map_sol_validated = regenerate_categories_map_helper(categories_map_sol_validated,
                                                                            de_te_additions_sol_validated,
                                                                            ip_version=ip_version)
        save_results_to_file(new_categories_map_sol_validated, str(save_directory),
                             'categories_map_sol_validated_updated_v{}'.format(ip_version))


if __name__ == '__main__':

    operation = str(sys.argv[1])

    if operation == 'g':
        # general
        # max_links_to_process 和 max_links_to_process_sol_validated 参数限制处理的链接数量，以便进行分块处理和并行计算。
        mode = int(sys.argv[2])
        ip_version = int(sys.argv[3])
        server_id = int(sys.argv[4])

        print(f'Generating cable mapping with mode = {mode}, ip_version = {ip_version} and at server_id = {server_id}')

        categories = ['bg_oc', 'og_oc', 'bb_oc', 'bg_te', 'og_te', 'bb_te']

        max_links_to_process, max_links_to_process_sol_validated = {}, {}

        for count, category in enumerate(categories):
            max_links_to_process[category] = int(sys.argv[count + 5])
            max_links_to_process_sol_validated[category] = int(sys.argv[count + 11])

        print(f'max_links_to_process : {max_links_to_process}')
        print(f'max_links_to_process_sol_validated : {max_links_to_process_sol_validated}')

        cable_mapping, cable_mapping_sol_validated = generate_cable_mapping(max_links_to_process,
                                                                            max_links_to_process_sol_validated,
                                                                            server_id, mode, ip_version,
                                                                            sol_threshold=0.05)

    if operation == 'n':
        # 普通版本，不用管那么多参数
        mode = int(sys.argv[2])
        ip_version = int(sys.argv[3])
        _, cable_mapping_sol_validated = generate_cable_mapping(mode=mode, ip_version=ip_version, sol_threshold=0.05)

    if operation == 'f':
        # final mapping
        generate_final_mapping(mode=1, ip_version=4, threshold=0.05)
        regenerate_categories_map(mode=1, ip_version=4)

    if operation == 't':
        # test
        cable_mapping, cable_mapping_sol_validated = generate_cable_mapping_test(mode=2, ip_version=4,
                                                                                 sol_threshold=0.05,
                                                                                 geolocation_threshold=0.6, ignore=True,
                                                                                 max_links_to_process=None,
                                                                                 max_links_to_process_sol_validated=None,
                                                                                 server_id=None)
        generate_final_mapping_test(cable_mapping, cable_mapping_sol_validated, mode=2, ip_version=4, threshold=0.05)
