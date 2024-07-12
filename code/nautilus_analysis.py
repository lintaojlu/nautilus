import json
import pickle
import re
import sys
from collections import namedtuple
from pathlib import Path

from fuzzywuzzy import fuzz
from tqdm import tqdm
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np

root_dir = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root_dir))
from code.utils.as_utils import get_ip_to_asn_for_all_ips, generate_as_mapping_based_on_caida_asrank_data

Location = namedtuple('Location', ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude',
                                   'autonomous_system_number', 'network', 'ISP', 'Org'])
MaxmindLocation = namedtuple('MaxmindLocation',
                             ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude',
                              'autonomous_system_number', 'network'])
LandingPoints = namedtuple('LandingPoints', ['latitude', 'longitude', 'country', 'location', 'cable'])
Cable = namedtuple('Cable', ['name', 'landing_points', 'length', 'owners', 'notes', 'rfs', 'other_info'])

root_dir = Path(__file__).resolve().parents[1]
file_dir = root_dir / 'stats'


# Custom Unpickler with updated class definitions
class CustomUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        if name == 'Cable':
            return Cable
        if name == 'LandingPoints':
            return LandingPoints
        return super().find_class(module, name)


# Define the functions to load each pickle file

def load_cable_info_dict(file_path):
    """
    Load the cable_info_dict from the pickle file.

    Data Structure:
    {
        cable_id: Cable(name, landing_points, length, owners, notes, rfs, other_info)
    }
    """
    with open(file_path, 'rb') as file:
        return CustomUnpickler(file).load()


def load_cable_to_connected_location_ids(file_path):
    """
    Load the cable_to_connected_location_ids from the pickle file.

    Data Structure:
    {
        cable_name: [[location_id, location_id, ...], ...]
    }
    """
    with open(file_path, 'rb') as file:
        return CustomUnpickler(file).load()


def load_country_dict(file_path):
    """
    Load the country_dict from the pickle file.

    Data Structure:
    {
        country_name: [cable_id, cable_id, ...]
    }
    """
    with open(file_path, 'rb') as file:
        return CustomUnpickler(file).load()


def load_landing_points_dict(file_path):
    """
    Load the landing_points_dict from the pickle file.

    Data Structure:
    {
        landing_point_id: LandingPoints(latitude, longitude, country, location, cable)
    }
    """
    with open(file_path, 'rb') as file:
        return CustomUnpickler(file).load()


def load_owners_dict(file_path):
    """
    Load the owners_dict from the pickle file.

    Data Structure:
    {
        owner_name: [cable_id, cable_id, ...]
    }
    """
    with open(file_path, 'rb') as file:
        return CustomUnpickler(file).load()


def load_link_cable_mapping(file_path):
    """
    Load the link to cable mapping from the pickle file.

    Data Structure:
    {
        link_id: (selected_cables_number, selected_cables_list, score_list, selected_landing_points_list, category)
    }
    """
    with open(file_path, 'rb') as file:
        return CustomUnpickler(file).load()


def save_ips_from_link_cable_mapping(link_cable_mapping, file_path):
    ips = set()
    for link, results in link_cable_mapping.items():
        ip1, ip2 = link
        ips.add(ip1)
        ips.add(ip2)
    with open(file_path, 'w') as f:
        for ip in ips:
            f.write(f'{ip}\n')
    print(f'IPs count: {len(ips)}')
    return ips


class CableInfo:
    def __init__(self):
        self.cable_name_to_cable_id = None
        self.cable_info_dict = None
        self.cable_to_connected_location_ids = None
        self.country_dict = None
        self.landing_points_dict = None
        self.owners_dict = None

        self.load_submarine_info()

    def load_submarine_info(self):
        self.cable_info_dict = load_cable_info_dict(file_dir / 'submarine_data' / 'cable_info_dict')
        self.cable_to_connected_location_ids = load_cable_to_connected_location_ids(
            file_dir / 'submarine_data' / 'cable_to_connected_location_ids')
        self.country_dict = load_country_dict(file_dir / 'submarine_data' / 'country_dict')
        self.landing_points_dict = load_landing_points_dict(file_dir / 'submarine_data' / 'landing_points_dict')
        self.owners_dict = load_owners_dict(file_dir / 'submarine_data' / 'owners_dict')

    def count_cable_coverage_and_mean_score(self, link_to_cable_mapping, top_n=1, cables_all=None):
        top_cables = {}
        for link_id, (cable_number, cable_list, score_list, lps_list, category) in link_to_cable_mapping.items():
            for cable in cable_list[:top_n]:
                score = score_list[cable_list.index(cable)]
                top_cables[cable] = top_cables.get(cable, []) + [score]

        # # Calculate the mean score for each cable
        # cable_mean_score = {cable: sum(scores) / len(scores) for cable, scores in top_cables.items()}
        total_score = 0
        total_count = 0
        for cable, scores in top_cables.items():
            total_score += sum(scores)
            total_count += len(scores)
        mean_score_all = total_score / total_count
        total_cables = len(self.cable_info_dict) if cables_all is None else len(cables_all)
        top_cables_count = len(top_cables)
        coverage = top_cables_count / total_cables
        print(
            f"Links num: {len(link_to_cable_mapping)}, Cables num: {len(top_cables)}, Coverage: {round(coverage * 100, 4)}%, Mean Score: {round(mean_score_all, 4)}")
        return coverage, mean_score_all

    def get_cable_name_to_cable_id(self):
        cable_name_to_cable_id = {}
        for cable_id, cable_info in self.cable_info_dict.items():
            cable_name_to_cable_id[cable_info.name] = cable_id
        self.cable_name_to_cable_id = cable_name_to_cable_id
        return cable_name_to_cable_id

    def find_std_cable_name(self, cable_name, threshold=60):
        std_cable_names = self.cable_name_to_cable_id.keys()
        results = []
        for std_cable_name in std_cable_names:
            if std_cable_name.lower() == cable_name.lower():
                results.append({std_cable_name: 100})
                break
            elif re.match(rf'.*\({cable_name.lower()}\).*', std_cable_name.lower()) is not None:
                results.append({std_cable_name: 99})
            else:
                similarity = fuzz.ratio(cable_name.lower(), std_cable_name.lower())  # 0-100
                if similarity >= threshold:
                    results.append({std_cable_name: similarity})
        results = sorted(results, key=lambda x: list(x.values())[0], reverse=True)
        print(cable_name, results)
        if results:
            std_cable = list(results[0].keys())[0]
        else:
            std_cable = None
        return std_cable


def get_ips_of_links(links):
    # links:{link_id: (selected_cables_number, selected_cables_list, score_list, selected_landing_points_list, category)}
    ips = set()
    for link, results in links.items():
        ip1, ip2 = link
        ips.add(ip1)
        ips.add(ip2)
    print(f'IPs count: {len(ips)}')
    return ips


def compare_two_str(name1, name2, threshold=90):
    return fuzz.ratio(name1.lower(), name2.lower()) >= threshold


def filter_links_with_org_name(links, ip_to_org, isp_name):
    isp_info = IspInfo()
    isp_all_names = isp_info.get_all_names_of_an_isp(isp_name)
    links_of_isp = {}
    for link, results in links.items():
        ip1, ip2 = link
        org1s = ip_to_org.get(ip1, [])
        org1_flag = False
        for org in org1s:
            for isp in isp_all_names:
                if compare_two_str(org, isp):
                    org1_flag = True
                    break
            if org1_flag:
                break
        org2s = ip_to_org.get(ip2, [])
        org2_flag = False
        for org in org2s:
            for isp in isp_all_names:
                if compare_two_str(org, isp):
                    org2_flag = True
                    break
            if org2_flag:
                break
        if org1_flag and org2_flag:
            links_of_isp[link] = results
    return links_of_isp


def filter_links_of_cables(links_isp, cables, top_n=0):
    cables = set(cables)
    links_of_cables = {}
    for link, results in links_isp.items():
        selected_cables_number, selected_cables_list, score_list, selected_landing_points_list, category = results
        if top_n > 0:
            top_n_cables = set(selected_cables_list[:top_n])
        else:
            top_n_cables = set(selected_cables_list)
        # top_n_cables isp_cables 交集是否为空
        if top_n_cables & cables:
            links_of_cables[link] = results
    return links_of_cables


def filter_links_higher_than_score(links, score_thresh=0.5):
    links_higher_than_score = {}
    for link, results in links.items():
        selected_cables_number, selected_cables_list, score_list, selected_landing_points_list, category = results
        if len(selected_cables_list) > 0 and score_list[0] > score_thresh:
            links_higher_than_score[link] = results
    print(f'Links higher than score {score_thresh} count: {len(links_higher_than_score)}')
    return links_higher_than_score


def get_ip_to_org_for_all_ips(ips):
    # ip to asn
    ip_to_asn = get_ip_to_asn_for_all_ips(ips)
    print(f'IP to ASN dict length: {len(ip_to_asn)}')

    # asn raw data
    asn_data = []
    save_file = root_dir / 'stats/asns.jsonl'
    with open(save_file) as fp:
        content = fp.readlines()
    for line in content:
        asn_data.append(json.loads(line))

    # org to asn
    org_to_asn_dict = {}
    # asn to org
    asn_to_org_dict = {}
    for item in asn_data:
        try:
            asn = item['asn']
            org_name = item['organization']['orgName']
            # Next we will fill the org to asn dict
            current_asns = org_to_asn_dict.get(org_name, [])
            current_asns.append(asn)
            org_to_asn_dict[org_name] = current_asns
            # Finally we fill the asn_name_to_org_dict
            current_orgs = asn_to_org_dict.get(asn, [])
            current_orgs.append(org_name)
            asn_to_org_dict[asn] = current_orgs
        except:
            continue
    print(f'ASN to Org dict length: {len(asn_to_org_dict)}')
    # print(json.dumps(asn_to_org_dict, indent=4))

    # ip to orgs
    ip_to_orgs = {}
    for ip, asn in ip_to_asn.items():
        orgs = asn_to_org_dict.get(asn, [])
        ip_to_orgs[ip] = orgs
        # print(f'{ip}: {asn}: {org}')
    print(f'IP to Org dict length: {len(ip_to_orgs)}')
    return ip_to_orgs


def validate_links_by_isp(links, isp_name, isp_cables: list):
    ips = get_ips_of_links(links)
    print('###### Get IP to Org for all IPs ######')
    ip_to_orgs = get_ip_to_org_for_all_ips(ips)
    print('###### Done IP to Org ######')

    links_isp = filter_links_with_org_name(links, ip_to_orgs, isp_name)
    links_isp_cable = filter_links_of_cables(links, isp_cables, 0)

    links_not_isp = links.keys() - links_isp.keys()
    links_not_isp_cable = links.keys() - links_isp_cable.keys()

    # get links keys
    links_isp_and_isp_cable = links_isp.keys() & links_isp_cable.keys()
    links_not_isp_and_isp_cable = links_not_isp & links_isp_cable.keys()
    links_not_isp_and_not_isp_cable = links_not_isp & links_not_isp_cable
    links_isp_and_not_isp_cable = links_isp.keys() & links_not_isp_cable

    # get links
    links_isp_and_isp_cable = {link: links_isp_cable[link] for link in list(links_isp_and_isp_cable)}
    links_not_isp_and_isp_cable = {link: links_isp_cable[link] for link in list(links_not_isp_and_isp_cable)}
    links_not_isp_and_not_isp_cable = {link: links[link] for link in list(links_not_isp_and_not_isp_cable)}
    links_isp_and_not_isp_cable = {link: links[link] for link in list(links_isp_and_not_isp_cable)}

    # analysis
    tp = len(links_isp_and_isp_cable)
    tp_mean_score = get_mean_score_of_links(links_isp_and_isp_cable)
    fp = len(links_not_isp_and_isp_cable)  # 不正确，因为当出现link映射到isp的海缆，但link不属于isp的情况时，原因可能时别的isp也用了这条海缆
    fp_mean_score = get_mean_score_of_links(links_not_isp_and_isp_cable)
    tn = len(links_not_isp_and_not_isp_cable)
    tn_mean_score = get_mean_score_of_links(links_not_isp_and_not_isp_cable)
    fn = len(links_isp_and_not_isp_cable)
    fn_mean_score = get_mean_score_of_links(links_isp_and_not_isp_cable)

    # show_line_bar([tp, tn, fp, fn], [tp_mean_score, tn_mean_score, fp_mean_score, fn_mean_score],
    #               ['TP', 'TN', 'FP', 'FN'], x_label='', y_label={'bar': '# of Link', 'line': 'Mean Score'},
    #               fig_name='ISP_Mapping.png')

    print(f'ISP `{isp_name}` links num: {len(links_isp)}')
    print(f'ISP `{isp_name}` cables links num: {len(links_isp_cable)}')
    print(f'ISP `{isp_name}` and ISP cables links num: {len(links_isp_and_isp_cable)}')
    recall = tp / (tp + fn)
    print(f'Recall: {round(recall, 4)}')

    # links_isp = {str(k): v for k, v in links_isp.items()}
    # links_validated = {str(k): v for k, v in links_isp_and_isp_cable.items()}
    # json.dump(links_isp, open(file_path.with_name(file_path.stem + '_isp.json'), 'w'), indent=4)
    # json.dump(links_validated, open(file_path.with_name(file_path.stem + '_validated.json'), 'w'), indent=4)

    return links_isp, links_isp_and_isp_cable, links_isp_and_not_isp_cable


def analyze_links_mapping(link_to_cables, file_suffix):
    # 统计映射数量、平均分
    # 按照海缆分布，统计数量、平均分 top_1_cables = {cable_name: {score_list: [], mean_score: 0, count: 0}}
    # 按照类别分布，统计数量、平均分 categories = {category: {score_list: [], mean_score: 0, count: 0}}
    top_1_cables = {}
    categories = {}
    for link, results in link_to_cables.items():
        selected_cables_number, selected_cables_list, score_list, selected_landing_points_list, category = results
        try:
            cable = selected_cables_list[0]
        except:
            continue
        top_1_cables[cable] = top_1_cables.get(cable, {'score_list': [], 'mean_score': 0, 'count': 0})
        top_1_cables[cable]['score_list'].append(score_list[0])
        top_1_cables[cable]['count'] += 1
        categories[category] = categories.get(category, {'score_list': [], 'mean_score': 0, 'count': 0})
        categories[category]['score_list'].append(score_list[0])
        categories[category]['count'] += 1
    for cable, info in top_1_cables.items():
        info['mean_score'] = sum(info['score_list']) / info['count']
    for category, info in categories.items():
        info['mean_score'] = sum(info['score_list']) / info['count']
    # 对于top_1_cables和categories按照mean_score排序
    top_1_cables = dict(sorted(top_1_cables.items(), key=lambda x: x[1]['mean_score'], reverse=True))
    categories = dict(sorted(categories.items(), key=lambda x: x[1]['mean_score'], reverse=True))
    # TODO 对于top_1_cables和categories分别作图，mean_score按照折线画出，count按照状图画出

    # 绘制 top_1_cables 图表
    cable_names = list(top_1_cables.keys())
    print(f'Result cables: {set(cable_names)}; ({len(set(cable_names))})')
    cable_counts = [info['count'] for info in top_1_cables.values()]
    cable_mean_scores = [info['mean_score'] for info in top_1_cables.values()]
    show_line_bar(cable_counts, cable_mean_scores, cable_names, x_label='Cable Name',
                  y_label={'bar': '# of Link', 'line': 'Mean Score'}, fig_name=f'Cable_Mapping_{file_suffix}.png')

    # 绘制 categories 图表
    category_names = list(categories.keys())
    category_counts = [info['count'] for info in categories.values()]
    category_mean_scores = [info['mean_score'] for info in categories.values()]
    show_line_bar(category_counts, category_mean_scores, category_names, x_label='Category',
                  y_label={'bar': '# of Link', 'line': 'Mean Score'}, fig_name=f'Category_Mapping_{file_suffix}.png')


def transform_cumulative(x_list):
    # 归一化
    x_sum = np.array(x_list).sum()
    # 求累计分布
    x_cumulative = [x_list[0] / x_sum]
    for i in range(1, len(x_list)):
        x_list[i] = x_list[i] + x_list[i - 1]
        x_cumulative.append(x_list[i] / x_sum)
    return x_cumulative


def show_cdf(data, x_label, y_label, fig_name):
    data = dict(sorted(data.items(), key=lambda x: x[1]))  # x[1] is the value
    x = list(data.values())
    y = list(data.values())
    y_cumulative = transform_cumulative(y)
    plt.plot(x, y_cumulative, linewidth='3', color="green", linestyle="solid")
    plt.xlabel(x_label, fontsize=20)
    plt.ylabel(y_label, fontsize=20)
    plt.xticks(fontsize=20, rotation=45)
    plt.yticks(fontsize=20)
    plt.tight_layout()
    plt.savefig(fig_name)
    plt.show()


def cal_links_per_cable(links_to_cables):
    """
    获取每条海缆对应的分数大于0.5的link的数量和平均分
    """
    cable_to_link_score = {}
    ips = set()
    for link, results in links_to_cables.items():
        selected_cables_number, selected_cables_list, score_list, selected_landing_points_list, category = results
        try:
            top_1_cable = selected_cables_list[0]
            if score_list[0] > 0.5:
                cable_to_link_score[top_1_cable] = cable_to_link_score.get(top_1_cable, []) + [score_list[0]]
                ips.add(link[0])
                ips.add(link[1])
        except:
            continue
    cable_to_link_count = {k: len(v) for k, v in cable_to_link_score.items()}
    mean_count = sum(cable_to_link_count.values()) / len(cable_to_link_count)
    cable_to_link_mean_score = {k: sum(v) / len(v) for k, v in cable_to_link_score.items()}
    mean_score = sum(cable_to_link_mean_score.values()) / len(cable_to_link_mean_score)
    print(f'Cables number: {len(cable_to_link_count)}')
    print(f'Cables: {cable_to_link_count.keys()}')
    print(f'IPs number: {len(ips)}')
    print(f'Average link count per cable: {mean_count}')
    print(f'Average link mean score per cable: {mean_score}')
    show_cdf(cable_to_link_count, 'Link Count', 'CDF', 'Cable_Link_Count_CDF.png')
    show_cdf(cable_to_link_mean_score, 'Mean Score', 'CDF', 'Cable_Mean_Score_CDF.png')
    return cable_to_link_count, cable_to_link_mean_score


def filter_links_with_type(links, param):
    links = {k: v for k, v in links.items() if v[-1] in param}
    return links


def links_with_cable_score(links, param, param1):
    results = {}
    for k, v in links.items():
        # print(k, v)
        try:
            if param < v[2][0] < param1:
                results[k] = v
        except TypeError:
            continue
    return results


def show_line_bar(bar_list, line_list, x_labels, legends=None, x_label='X Axis',
                  y_label=None, fig_name='Combined_Plot.png'):
    if y_label is None:
        y_label = {'bar': 'Bar Y Axis', 'line': 'Line Y Axis'}
    if legends is None:
        legends = y_label
    if not isinstance(bar_list[0], list):
        bar_list = [bar_list]
    if not isinstance(line_list[0], list):
        line_list = [line_list]
    if len(x_labels[0]) > 4:
        rotation = 45
    else:
        rotation = 0

    fig, ax1 = plt.subplots(figsize=(15, 9))
    # plt.rc('font', family='Times New Roman')

    ax2 = ax1.twinx()

    x = np.arange(len(bar_list[0]))  # x轴刻度标签位置
    width = 0.35

    # 绘制柱状图
    ecs = ['royalblue', 'tomato', 'green', 'purple']
    lw = 2
    color = 'w'
    hatches = ['x', '*', '-']

    for i, bar_values in enumerate(bar_list):
        bars = ax1.bar(x + i * width, bar_values, width, label=legends['bar'][i], lw=lw, color=color, hatch=hatches[i],
                       ec=ecs[i])
        # # 在柱状图上显示具体数值
        # for bar in bars:
        #     yval = bar.get_height()
        #     ax1.text(bar.get_x() + bar.get_width() / 2, yval, round(yval, 2), ha='center', va='bottom', fontsize=50)

    ax1.set_xlabel(x_label, fontsize=67)
    ax1.set_ylabel(y_label['bar'], fontsize=67)
    ax1.set_xticks(x)
    ax1.set_xticklabels(x_labels, fontsize=30, rotation=rotation)
    ax1.tick_params(axis='y', labelsize=45)  # Increase y-axis tick label font size for bar chart
    # ax1.legend(loc='upper right', fontsize=40)
    ax1.grid(True, linestyle=':', color='b', alpha=0.6)

    # 绘制折线图
    colors = [[0.8196078431372549, 0.3411764705882353, 0.37254901960784315],
              [0.6313725490196078, 0.611764705882353, 0.8823529411764706],
              [0.0784313725490196, 0.5568627450980392, 0.7803921568627451],
              'purple']
    hatches = ['D', 'o', 's']

    for i, y_values in enumerate(line_list):
        ax2.plot(np.arange(len(y_values)), y_values, marker=hatches[i], markersize=30, linestyle='--', linewidth=5,
                 color=colors[i],
                 label=legends['line'][i])
        # 在折线图上显示具体数值
        for x_val, y_val in enumerate(y_values):
            ax2.text(x_val, y_val, round(y_val, 2), ha='center', va='bottom', fontsize=40)

    ax2.set_ylabel(y_label['line'], fontsize=67)
    ax2.tick_params(axis='y', labelsize=45)  # Increase y-axis tick label font size for bar chart
    # ax2.legend(loc='upper right', fontsize=40)

    # plt.title(fig_name, fontsize=67)
    plt.tight_layout()
    plt.savefig(fig_name, bbox_inches='tight')
    # plt.show()


def show_bar(bar_list, x_labels, legends=None, x_label='X Axis',
             y_label='Bar Y Axis', fig_name='Bar_Plot.png'):
    if legends is None:
        legends = ['Bar1']
    if not isinstance(bar_list[0], list):
        bar_list = [bar_list]
    if len(x_labels[0]) > 4:
        rotation = 45
    else:
        rotation = 0

    fig, ax = plt.subplots(figsize=(15, 9))
    # plt.rc('font', family='Times New Roman')

    x = np.arange(len(bar_list[0]))  # x轴刻度标签位置
    width = 0.35

    # 绘制柱状图
    ecs = ['royalblue', 'tomato', 'green', 'purple']
    lw = 2
    color = 'w'
    hatches = ['x', '*', '-']

    for i, bar_values in enumerate(bar_list):
        bars = ax.bar(x + i * width, bar_values, width, label=legends[i], lw=lw, color=color, hatch=hatches[i],
                      ec=ecs[i])
        # 在柱状图上显示具体数值
        for bar in bars:
            yval = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, yval, round(yval, 2), ha='center', va='bottom', fontsize=20)

    ax.set_xlabel(x_label, fontsize=67)
    ax.set_ylabel(y_label, fontsize=67)
    ax.set_xticks(x)
    ax.set_xticklabels(x_labels, fontsize=50, rotation=rotation)
    ax.tick_params(axis='y', labelsize=50)  # Increase y-axis tick label font size
    ax.legend(loc='upper right', fontsize=40)
    ax.grid(True, linestyle=':', color='b', alpha=0.6)

    plt.tight_layout()
    plt.savefig(fig_name, bbox_inches='tight')
    plt.show()


def get_mean_score_of_links(links):
    scores = []
    for link, results in links.items():
        try:
            score = results[2][0]
            scores.append(score)
        except:
            continue
    mean_score = sum(scores) / len(scores) if scores else 0
    return mean_score


def estimate_precision(links, score_thresh, file_suffix):
    links_oc = filter_links_with_type(links, ['bg_oc', 'og_oc', 'bb_oc'])
    links_te = filter_links_with_type(links, ['bg_te', 'og_te', 'bb_te', 'de_te'])
    # de_te shi shen me
    links_positive = links_with_cable_score(links, score_thresh, 1)
    links_negative = links_with_cable_score(links, 0, score_thresh)

    tp = links_positive.keys() & links_oc.keys()
    tp_links = {str(k): v for k, v in links_positive.items() if k in tp}
    tp_mean_score = get_mean_score_of_links(tp_links)

    tn = links_negative.keys() & links_te.keys()
    tn_links = {str(k): v for k, v in links_negative.items() if k in tn}
    tn_mean_score = get_mean_score_of_links(tn_links)

    fp = links_positive.keys() & links_te.keys()
    fp_links = {str(k): v for k, v in links_negative.items() if k in fp}
    fp_mean_score = get_mean_score_of_links(fp_links)

    fn = links_negative.keys() & links_oc.keys()
    fn_links = {str(k): v for k, v in links_negative.items() if k in fn}
    fn_mean_score = get_mean_score_of_links(fn_links)

    count_list = [len(tp), len(tn), len(fp), len(fn)]
    mean_score_list = [tp_mean_score, tn_mean_score, fp_mean_score, fn_mean_score]
    labels = ['TP', 'TN', 'FP', 'FN']

    show_line_bar(count_list, mean_score_list, labels, x_label='', y_label={'bar': '# of Link', 'line': 'Mean Score'},
                  fig_name='Precision_Mapping.png')
    precision = len(tp) / (len(tp) + len(fp))
    recall = len(tp) / (len(tp) + len(fn))
    accuracy = (len(tp) + len(tn)) / (len(tp) + len(tn) + len(fp) + len(fn))

    print(f'TP: {len(tp)}, TN: {len(tn)}, FP: {len(fp)}, FN: {len(fn)}')
    print(f'Precision: {precision}')
    print(f'Recall: {recall}')
    print(f'Accuracy: {accuracy}')
    x_list = [precision, recall, accuracy]
    x_labels = ['Precision', 'Recall', 'Accuracy']
    show_bar(x_list, x_labels, x_label='Metrics', y_label='Value', fig_name=f'Metrics_Plot_{file_suffix}.png')


def show_cable_segments_on_map(df: pd.DataFrame, shapefile_path: str):
    def bezier_curve(p0, p1, p2, t):
        return (1 - t) ** 2 * p0 + 2 * (1 - t) * t * p1 + t ** 2 * p2

    # Load the world map from the shapefile
    world = gpd.read_file(shapefile_path)

    # Create a plot with the world map
    fig, ax = plt.subplots(figsize=(15, 10))
    world.plot(ax=ax, color='lightgrey')

    # Iterate over each row in the dataframe
    for index, row in df.iterrows():
        cable_name = row['cable_name']
        lp1_coordinates = row['lp1_coordinates']
        lp2_coordinates = row['lp2_coordinates']
        number = row['number']

        # Extract coordinates
        y1, x1 = lp1_coordinates
        y2, x2 = lp2_coordinates

        # Calculate control point for the Bezier curve (simple midpoint control point with a small offset)
        control_point = ((x1 + x2) / 2, (y1 + y2) / 2 + np.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2) * 0.2)

        # Generate points for the Bezier curve
        t = np.linspace(0, 1, 500)
        x = bezier_curve(x1, control_point[0], x2, t)
        y = bezier_curve(y1, control_point[1], y2, t)

        # Plot the Bezier curve
        ax.plot(x, y, linewidth=number, label=cable_name)

        # Annotate the cable name at the midpoint of the curve
        mid_idx = len(t) // 2
        # ax.text(x[mid_idx], y[mid_idx], cable_name, fontsize=12, ha='center')

    # Add plot title and labels
    ax.set_title('Submarine Cable Segments')
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def visualize_cables_on_map(links_cable_mapping):
    cable_segment_to_number = {}
    for link, results in links_cable_mapping.items():
        selected_cables_number, selected_cables_list, score_list, selected_landing_points_list, category = results
        if len(selected_cables_list) == 0:
            continue
        for lp_pair in selected_landing_points_list[0]:
            cable_segment = selected_cables_list[0] + '_' + str(lp_pair[0]) + '_' + str(lp_pair[1])
            cable_segment_to_number[cable_segment] = cable_segment_to_number.get(cable_segment, 0) + 1
    cable_segment_to_number = dict(sorted(cable_segment_to_number.items(), key=lambda x: x[1], reverse=True))
    print(f'Cable segments count: {len(cable_segment_to_number)}')
    cable_count = len(set([cable.split("-")[0] for cable in cable_segment_to_number.keys()]))
    print(f'Cable count: {cable_count}')
    print(cable_segment_to_number)
    # convert to dataframe
    cable_segment_number = []
    nautilus = CableInfo()
    for cable_segment, number in cable_segment_to_number.items():
        cable_name, lp1, lp2 = cable_segment.split('_')
        lp1_coordinates = (
            nautilus.landing_points_dict[int(lp1)].latitude, nautilus.landing_points_dict[int(lp1)].longitude)
        lp2_coordinates = (
            nautilus.landing_points_dict[int(lp2)].latitude, nautilus.landing_points_dict[int(lp2)].longitude)
        cable_segment_number.append([cable_name, lp1, lp2, number, lp1_coordinates, lp2_coordinates])
    df = pd.DataFrame(cable_segment_number,
                      columns=['cable_name', 'lp1', 'lp2', 'number', 'lp1_coordinates', 'lp2_coordinates'])
    shapefile_path = '/Users/linsir/Experiments/PyCharm/nautilus-main/stats/IPUMSI_world_release2024/IPUMSI_world_release2024.shp'
    show_cable_segments_on_map(df, shapefile_path)


def convert_pickle_result_to_df(links):
    nautilus = CableInfo()
    # header = [src_ip, dst_ip, cable_name, landing_point_a, landing_point_b, score, category]
    data = []
    for link, results in tqdm(links.items()):
        src_ip, dst_ip = link
        selected_cables_number, selected_cables_list, score_list, selected_landing_points_list, category = results
        for i in range(len(selected_cables_list)):
            cable_name = selected_cables_list[i]
            for j in range(len(selected_landing_points_list[i])):
                lp1, lp2 = selected_landing_points_list[i][j]
                score = score_list[i]
                lp1_location = nautilus.landing_points_dict[int(lp1)].location
                lp2_location = nautilus.landing_points_dict[int(lp2)].location
                data.append([src_ip, dst_ip, cable_name, lp1_location, lp2_location, score, category])
    df = pd.DataFrame(data,
                      columns=['src_ip', 'dst_ip', 'cable_name', 'lp1_location', 'lp2_location', 'score', 'category'])
    return df


def analyze_and_validate_by_isp(links, isp_name):
    top_n = 1
    nautilus = CableInfo()
    isp_info = IspInfo()
    isp_cables = isp_info.get_isp_cables(isp_name)

    # print('Analyze raw links mapping')
    # coverage, mean_score = nautilus.count_cable_coverage_and_mean_score(links, top_n)
    # analyze_links_mapping(links, f'{isp_name}_raw')

    # print('Estimate precision')
    # estimate_precision(links, 0.4, f'{isp_name}_raw_0.4')
    # estimate_precision(links, 0.5, f'{isp_name}_raw_0.5')
    # estimate_precision(links, 0.6, f'{isp_name}_raw_0.6')
    # estimate_precision(links, 0.7, f'{isp_name}_raw_0.7')

    print('Analyze links mapping with OC cables')
    links_oc = filter_links_with_type(links, ['bg_oc', 'og_oc', 'bb_oc'])
    coverage, mean_score = nautilus.count_cable_coverage_and_mean_score(links_oc, top_n)
    analyze_links_mapping(links_oc, f'{isp_name}_oc')

    # print(f'Validating links by ISP `{isp_name}`')
    # links_isp, links_validated, links_uncorrected = validate_links_by_isp(links, isp_name, isp_cables)
    # print('links isp:')
    # for link, results in links_isp.items():
    #     print(link, results)
    # print()

    print(f'Validating links by ISP `{isp_name}`(xx_oc)')
    links_isp, links_validated, links_uncorrected = validate_links_by_isp(links_oc, isp_name, isp_cables)

    # save links of the isp to csv
    # links_isp = filter_links_higher_than_score(links_isp, 0.4)
    # df_links_isp = convert_pickle_result_to_df(links_isp)
    # # sort by score
    # df_links_isp = df_links_isp.sort_values(by=['score'], ascending=False)
    # df_links_isp.to_csv('link_cable_mapping.csv', index=False)

    print(f"Analyze links mapping that validated by ISP `{isp_name}`")
    nautilus.count_cable_coverage_and_mean_score(links_validated, top_n, cables_all=isp_cables)
    analyze_links_mapping(links_validated, f'{isp_name}_validated')
    # print('links validated:')
    # for link, results in links_validated.items():
    #     print(link, results)
    # print()

    # print('Cal links per cable')
    # cal_links_per_cable(links)

    print(f"Analyzing links mapping that not validated by ISP `{isp_name}`")
    nautilus.count_cable_coverage_and_mean_score(links_uncorrected, top_n, cables_all=isp_cables)
    analyze_links_mapping(links_uncorrected, f'{isp_name}_uncorrected')
    print('links uncorrected:')
    for link, results in links_uncorrected.items():
        print(link, results)
    print()
    print(len(filter_links_higher_than_score(links_uncorrected, 0)))
    print(len(filter_links_with_type(filter_links_higher_than_score(links_uncorrected, 0), ['og_te'])))


class IspInfo:
    def __init__(self):
        self.isp_cable_info = json.load(open(root_dir / 'stats' / 'submarine_data' / 'isp_cables.json', 'r'))

    def get_isp_cables(self, isp_name):
        cable_name_list = self.isp_cable_info[isp_name]
        cable_id_list = [cable_name.lower().replace(' ', '-') for cable_name in cable_name_list]
        cable_list = cable_id_list + cable_name_list
        return cable_list

    def get_all_names_of_an_isp(self, isp_name):
        if isp_name == 'TATA':
            return ['Tata', 'TATA', 'Tata Communications Limited', 'TATA COMMUNICATIONS (AMERICA) INC',
                    'Tata Teleservices (Maharashtra) Ltd', 'TATA Communications Internet Services Ltd',
                    'Tata Institute of Fundamental Research']
        if isp_name == 'China Telecom':
            return ['China Telecom', 'CHINA TELECOM', 'PT. CHINA TELECOM INDONESIA']
        else:
            return [isp_name]

    def save_isp_info(self):
        json.dump(self.isp_cable_info, open(root_dir / 'stats' / 'submarine_data' / 'isp_cables.json', 'w'), indent=4)
        print('ISP cables info saved')

    def replace_cables_with_std_name(self, isp_name):
        isp_cables = self.isp_cable_info[isp_name]
        print('Convert raw cable names to standard cable names')
        nautilus = CableInfo()
        for cable_raw_name in isp_cables:
            nautilus.get_cable_name_to_cable_id()
            std_cable = nautilus.find_std_cable_name(cable_raw_name, 70)
            if std_cable:
                print(f'{cable_raw_name} -> {std_cable}')
                isp_cables.remove(cable_raw_name)
                isp_cables.append(std_cable)
        self.isp_cable_info[isp_name] = isp_cables
        self.save_isp_info()


def get_ips_to_monitor(links):
    # links:{link_id: (selected_cables_number, selected_cables_list, score_list, selected_landing_points_list, category)}
    links = filter_links_higher_than_score(links, 0.7)
    save_ips_from_link_cable_mapping(links, 'ips_to_monitor.txt')


if __name__ == '__main__':
    # isp = 'TATA'
    # # isp = 'China Telecom'
    # links_path = root_dir / 'stats' / 'mapping_outputs' / 'link_to_cable_and_score_mapping_sol_validated_v4'
    # # links_path = '/home/lintao/scinfer/data/result/link/mine_scored.pkl'
    # print(f'Loading link cable mapping from {links_path}')
    # # format: {(8.8.8.8,10.10.10.10): 3, [sc1, faster, jupiter], [0.9, 0.8, 0.7], [[[lp1, lp2], [lp2], [lp3]], [[lp2], [lp3]], []], category}
    # links_cable_mapping = load_link_cable_mapping(links_path)
    # print(f'Link cable mapping length: {len(links_cable_mapping)}')
    # # links_cable_mapping = filter_links_higher_than_score(links_cable_mapping, 0)
    # print(f'Link cable mapping length of score higher than 0: {len(links_cable_mapping)}')
    # analyze_and_validate_by_isp(links_cable_mapping, isp)
    # # visualize_cables_on_map(links_cable_mapping)
    #
    # # get_ips_to_monitor(links_cable_mapping)
    data = load_link_cable_mapping(root_dir/ 'stats' / 'mapping_outputs' / 'cable_mapping_bg_oc_v4')
    for k, v in data.items():
        print(k, v)
        input('continue')