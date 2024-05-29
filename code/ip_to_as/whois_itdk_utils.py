import datetime
import os
import pickle

from tqdm import tqdm
from pathlib import Path

root_directory = Path(__file__).parent.parent.parent


class ITDK:
    """
    ITDK类用于处理ITDK数据集，包括下载、解析和统计
    最后得到的ip到geo的映射存储在ip_to_geo字典中，格式为：
    ip_to_geo[ip] = [country, city, lat, lon]
    """

    # 初始化ITDK类
    def __init__(self, root_dir: str):
        # 设置文件根目录
        self.root_dir = root_dir
        # 设置文件输出目录，这个暂时采用硬编码
        self.ip_to_as_path = root_directory / 'stats/location_data/caida_ip_to_as_map'
        self.ip_to_geo_path = os.path.join(self.root_dir, 'ip_to_geo.csv')
        os.makedirs(self.root_dir, exist_ok=True)
        # 设置数据集的URL
        self.url = 'https://publicdata.caida.org/datasets/topology/ark/ipv4/itdk'
        # 初始化存储拓扑信息的字典
        self.topo = dict()
        # 初始化存储IP到地理信息映射的字典
        self.ip_to_geo = dict()
        # 初始化其他数据结构
        self.node_to_ip = dict()
        self.as_info = dict()
        self.ip_to_as = dict()

    # 下载指定日期的ITDK数据
    def download(self, date, kapar=False):
        # 检查指定日期的数据目录是否存在，如果不存在则创建
        date_dir = os.path.join(self.root_dir, date)
        if not os.path.exists(date_dir):
            os.system(f"mkdir {date_dir}")
        # 获取当前目录下的文件列表
        existed = os.listdir(date_dir)
        # 如果所需的文件已经存在，则不再下载
        if 'midar-iff.nodes.geo' in existed:
            print(f'[{datetime.datetime.now()}] ITDK files existed')
            return

        # 如果文件不存在，则重新下载
        os.chdir(date_dir)

        # 清空当前目录下的文件，小心使用rm -rf！
        os.system(f'rm -r ./*')
        print(f'Begin Download: {date}')
        # 根据日期判断文件的压缩格式，并下载相应的文件
        if date < '2013-07':
            # 2013年7月之前的文件是gzip压缩格式
            # 下载和解压.nodes文件
            if not kapar:
                url = f"{self.url}/{date}/midar-iff.nodes.gz"
                os.system(f"wget {url}")
                os.system(f"gzip -d midar-iff.nodes.gz")
            else:
                url = f"{self.url}/{date}/kapar-midar-iff.nodes.gz"
                os.system(f"wget {url}")
                os.system(f"gzip -d kapar-midar-iff.nodes.gz")

            # 下载和解压.nodes.geo文件
            if not kapar:
                url = f"{self.url}/{date}/midar-iff.nodes.geo.gz"
                os.system(f"wget {url}")
                os.system(f"gzip -d midar-iff.nodes.geo.gz")
            else:
                url = f"{self.url}/{date}/kapar-midar-iff.nodes.geo.gz"
                os.system(f"wget {url}")
                os.system(f"gzip -d kapar-midar-iff.nodes.geo.gz")

            # 下载和解压.nodes.as文件
            if not kapar:
                url = f"{self.url}/{date}/midar-iff.nodes.as.gz"
                os.system(f"wget {url}")
                os.system(f"gzip -d midar-iff.nodes.as.gz")
            else:
                url = f"{self.url}/{date}/kapar-midar-iff.nodes.as.gz"
                os.system(f"wget {url}")
                os.system(f"gzip -d kapar-midar-iff.nodes.as.gz")
        else:
            # 2013年7月之后的文件是bzip2压缩格式
            # 下载和解压.nodes文件
            if not kapar:
                url = f"{self.url}/{date}/midar-iff.nodes.bz2"
                os.system(f"wget {url}")
                os.system(f"bzip2 -d midar-iff.nodes.bz2")
            else:
                url = f"{self.url}/{date}/kapar-midar-iff.nodes.bz2"
                os.system(f"wget {url}")
                os.system(f"bzip2 -d kapar-midar-iff.nodes.bz2")

            # 下载和解压.nodes.geo文件
            if not kapar:
                url = f"{self.url}/{date}/midar-iff.nodes.geo.bz2"
                os.system(f"wget {url}")
                os.system(f"bzip2 -d midar-iff.nodes.geo.bz2")
            else:
                url = f"{self.url}/{date}/kapar-midar-iff.nodes.geo.bz2"
                os.system(f"wget {url}")
                os.system(f"bzip2 -d kapar-midar-iff.nodes.geo.bz2")

            # 下载和解压.nodes.as文件
            if not kapar:
                url = f"{self.url}/{date}/midar-iff.nodes.as.bz2"
                os.system(f"wget {url}")
                os.system(f"bzip2 -d midar-iff.nodes.as.bz2")
            else:
                url = f"{self.url}/{date}/kapar-midar-iff.nodes.as.bz2"
                os.system(f"wget {url}")
                os.system(f"bzip2 -d kapar-midar-iff.nodes.as.bz2")
        # 完成下载后返回上级目录
        os.chdir('../../../../')
        print(f'cwd: {os.getcwd()}')

    # 加载IP到地理信息的映射
    def load_ip_to_geo(self, date):
        data_dir = os.path.join(self.root_dir, date)
        # 打开并读取.nodes文件，提取节点ID和IP地址信息
        print(f'[{datetime.datetime.now()}] Loading Nodes...')
        try:
            open_file = open(f"{data_dir}/midar-iff.nodes", 'r')
        except FileNotFoundError:
            print(f"[{datetime.datetime.now()}] File Not Found: {data_dir}/midar-iff.nodes")
            return
        for line in open_file:
            if line.startswith('#'):
                continue
            elems = line.strip().split(' ')
            node_id = elems[1][:-1]
            ips = elems[3:]
            print(node_id, end='\r', flush=True)

            # 在拓扑字典中创建新条目，包含IP地址和地理信息
            self.topo[node_id] = [[], []]
            self.topo[node_id][0] = ips
        print(f"[{datetime.datetime.now()}] Load Nodes Done, # of Routers: {len(self.topo.keys())}")
        open_file.close()

        # 打开并读取.nodes.geo文件，提取节点的地理信息
        print(f"[{datetime.datetime.now()}] Loading Geos...")
        open_file = open(os.path.join(data_dir, 'midar-iff.nodes.geo'), 'r', errors='ignore')
        for line in open_file:
            if line.startswith('#'):
                continue
            elems = line.strip().split('\t')
            if len(elems) < 7:
                continue
            node_id = elems[0].split(' ')[-1][:-1]
            print(node_id, end='\r', flush=True)
            try:
                country = elems[2].replace(',', '-')
                city = elems[4].replace(',', '-')
                lat = float(elems[5])
                lon = float(elems[6])
            except (ValueError, TypeError, IndexError) as e:
                print(f"[{datetime.datetime.now()}] Error with {node_id}: {e}")
                continue

            if node_id in self.topo.keys() and lat and lon:
                self.topo[node_id][1] = [country, city, lat, lon]
        open_file.close()
        print(f"[{datetime.datetime.now()}] Load Geos Done.")

        self.statistic()

        # 遍历拓扑字典，将每个IP地址映射到其地理信息
        print(f"[{datetime.datetime.now()}] Init IP to GEO...")
        node_ids = list(self.topo.keys())
        for node_id in node_ids:
            ips, geo = self.topo[node_id]
            if not geo:
                continue
            for ip in ips:
                self.ip_to_geo[ip] = geo
            del self.topo[node_id]  # 删除已处理的拓扑项以节省内存
        print(f"[{datetime.datetime.now()}] Init IP to GEO Done.")

    # 统计信息函数
    def statistic(self):
        if not self.topo:
            print(f"[{datetime.datetime.now()}] Topo is None.")
            return
        print(f"[{datetime.datetime.now()}] Begin Statistics")
        num_routers = len(self.topo.keys())
        print(f"[{datetime.datetime.now()}] # of Routers:       {num_routers}")
        num_ips = len(self.ip_to_geo.keys())
        print(f"[{datetime.datetime.now()}] # of IPs:       {num_ips}")

    # 保存IP到地理信息的映射
    def save_ip_to_geo(self):
        if not self.ip_to_geo:
            print(f"[{datetime.datetime.now()}] IP to GEO is None.")
            return
        open_file = open(self.ip_to_geo_path, 'w')
        open_file.write("ip,country,city,latitude,longitude\n")
        for ip in self.ip_to_geo.keys():
            try:
                country, city, lat, lon = self.ip_to_geo.get(ip, ("", "", "", ""))
            except (ValueError, TypeError, IndexError, KeyError, AttributeError) as e:
                print(f"[{datetime.datetime.now()}] Error with {ip}: {e}")
                continue
            open_file.write(f"{ip},{country},{city},{lat},{lon}\n")
        open_file.close()
        print(f"[{datetime.datetime.now()}] Save IP to GEO Done.")

    # 加载节点到IP的映射
    def load_node_to_ip(self, date):
        data_dir = os.path.join(self.root_dir, date)
        with open(os.path.join(data_dir, 'midar-iff.nodes'), 'r') as file:
            for line in tqdm(file, desc='Loading node to IP mapping', unit=' lines'):
                if line.startswith('#'):
                    continue
                elems = line.strip().split(' ')
                node_id = elems[1][:-1]
                ips = elems[3:]
                self.node_to_ip[node_id] = ips
        print(f'Loaded node to IP mapping: {len(self.node_to_ip)} nodes.')

    # 加载节点到AS的映射
    def load_node_to_as(self, date):
        data_dir = os.path.join(self.root_dir, date)
        with open(os.path.join(data_dir, 'midar-iff.nodes.as'), 'r') as file:
            for line in tqdm(file, desc='Loading node to AS mapping', unit=' lines'):
                if line.startswith('#'):
                    continue
                elems = line.strip().split('\t')
                node_id = elems[1]
                asn = elems[2]
                self.as_info[node_id] = asn
        print(f'Loaded node to AS mapping: {len(self.as_info)} nodes.')

    # 加载IP到AS的映射
    def load_ip_to_as(self, date):
        if os.path.exists(self.ip_to_as_path):
            with open(self.ip_to_as_path, 'rb') as file:
                self.ip_to_as = pickle.load(file)
            print(f'Loaded IP to AS mapping: {len(self.ip_to_as)} IPs.')
            return self.ip_to_as

        self.load_node_to_ip(date)
        self.load_node_to_as(date)
        for node_id, ips in tqdm(self.node_to_ip.items(), desc='Loading IP to AS mapping', unit=' nodes'):
            asn = self.as_info.get(node_id)
            if asn:
                for ip in ips:
                    self.ip_to_as[ip] = asn
        print(f'Loaded IP to AS mapping: {len(self.ip_to_as)} IPs.')
        self.save_ip_to_as()
        return self.ip_to_as

    # 保存IP到AS的映射
    def save_ip_to_as(self):
        if not self.ip_to_as:
            print(f"IP to AS mapping is empty.")
            return
        with open(self.ip_to_as_path, 'wb') as file:
            pickle.dump(self.ip_to_as, file)
        print(f'Saved IP to AS mapping to {self.ip_to_as_path}.')


def save_whois_itdk_output(output, ip_version=4, tags='default'):
    save_directory = root_directory / 'stats/ip2as_data/'

    save_directory.mkdir(parents=True, exist_ok=True)

    save_file = save_directory / 'caida_whois_output_v{}_{}'.format(ip_version, tags)

    with open(save_file, 'wb') as fp:
        pickle.dump(output, fp)


def generate_ip2as_for_list_of_ips(ip_version=4, list_of_ips=None, tags='default'):
    if list_of_ips is None:
        return
    whois_output = {}
    itdk = ITDK(str(root_directory / 'stats/location_data/caida_itdk_files'))
    ip_to_as = itdk.load_ip_to_as('2022-02')
    for ip in tqdm(list_of_ips, desc='CAIDA whois', total=len(list_of_ips)):
        whois_output[ip] = ip_to_as.get(ip, None)
    save_whois_itdk_output(whois_output, ip_version, tags)
    return whois_output


if __name__ == '__main__':
    ip_version = 4
    with open(root_directory / f'stats/mapping_outputs/all_ips_v{ip_version}', 'rb') as fp:
        list_of_ips = pickle.load(fp)
    # list_of_ips = ['66.85.82.9', '156.225.182.1', '67.59.254.241', '103.78.227.1', '193.34.197.140', '23.111.226.1', '193.0.214.1', '152.255.147.235', '216.19.218.1']

    generate_ip2as_for_list_of_ips(ip_version, list_of_ips, 'default')
