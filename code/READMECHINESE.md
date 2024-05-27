# 生成 Nautilus 映射

Nautilus 映射分为 (i) 预处理 和 (ii) 实际映射阶段。这样做是为了确保由于可能需要执行的大量测量（即数百万次）能够有效进行。

注意：所有文件需要从代码库的根目录（即 nautilus/code/ 目录）运行，以确保正确访问所有中间文件表示。

## 预处理步骤

### 生成 traceroute

Nautilus 依赖于 2 个来源的 traceroute： (i) RIPE Atlas — 5051 和 5151 用于 IPv4，6052 和 6152 用于 IPv6，(ii) CAIDA — /24 和 /48 前缀探测分别用于 IPv4 和 IPv6

#### 生成 RIPE Atlas traceroute

要在给定时间范围内生成 traceroute，可以使用以下代码段。以下是 2022 年 3 月 15 日至 29 日之间的 5151 测量 ID 的代码示例。相关函数在 “traceroute/ripe_traceroute_utils.py” 文件中。

```python
start_time = datetime(2022, 3, 15, 0)
end_time = datetime(2022, 3, 29, 0)
ripe_process_traceroutes(start_time, end_time, '5151', 4, False)
```

在上述函数中，4 表示 IP 版本，对于 traceroute 收集，初始阶段最后一个参数应设置为 False。此操作的结果将保存为文件 “stats/ripe_data/uniq_ip_dict_5151_…..”

注意：前面的操作会生成多个文件，但我们只需要最终生成的文件，通常文件名末尾带有 “<date>_count_”。例如，上述操作将生成文件 “uniq_ip_dict_5151_all_links_v4_min_all_latencies_only_03_29_2022_00_00_count_28”，该文件将用于进一步处理。

#### 生成 CAIDA traceroute

要生成 CAIDA 的 traceroute，需要使用特定周期 ID 而不是时间范围。例如，对于 2022 年 3 月 13 日至 23 日的时间范围，对应的周期 ID 是 1647。为此，生成 traceroute 的代码段如下。相关函数在 “traceroute/caida_traceroute_utils.py” 文件中。

```python
caida_process_traceroutes(2022, 3, 1647, 4, 1000, False)
```

1647 对应于周期 ID，2022 和 3 分别是周期的年份和月份，4 表示 IP 版本，最后两个参数在初始 traceroute 生成阶段可以保持默认值。

注意：运行以下代码段时，需要安装 CAIDA 的 “scamper” 工具来处理从 CAIDA 服务器下载的 warts 文件。最终生成的文件将保存在 “stats/caida_data/uniq_ip_dict_caida_all_links_v4_min_all_latencies_only_….”

注意：对于运行 CAIDA 的 /24 探测（IPv4），需要访问权限，代码会提示输入用户名和密码，这需要提供以执行和处理 CAIDA traceroute。对于 IPv6，由于数据是公开的，如果提供了测量 ID，Nautilus 将自动提取并处理所需的 traceroute。

### 生成所有唯一 IP 和链路

最后，一旦生成了来自 RIPE 和 CAIDA 的 traceroute，我们需要获取所有相关的 IP 端点和链路。代码段如下所示，函数位于 “utils/traceroute_utils.py” 文件中。

```python
load_all_links_and_ips_data(ip_version=4)
```

这将在 “stats/mapping_outputs” 目录中生成 “all_ips_v4” 和 “links_v4”，分别对应于唯一 IP 和链路的列表。

### 生成地理定位结果

#### RIPE 地理定位

首先运行地理定位脚本，需要从 RIPE ftp 服务器下载必要的 RIPE 地理定位文件，并将其放置在 “stats/location_data” 目录中。代码段如下，函数详情见 “location/ripe_geolocation_utils.py” 文件。

```python
links, ips = load_all_links_and_ips_data(ip_version=4)
generate_location_for_list_of_ips_ripe(ips, ip_version=4)
```

此代码段在 “stats/location_data” 目录中生成 RIPE 地理定位结果，文件名为 “ripe_location_output_v4_default”。

#### CAIDA 地理定位

同样，对于 CAIDA，需要从 CAIDA Ark 平台下载 midar.iff 和 midar.iff.nodes.geo 文件，并将其放置在 “stats/location_data” 目录中。获取 CAIDA 地理定位的代码段如下，函数详情见 “location/caida_geolocation_utils.py” 文件。

```python
links, ips = load_all_links_and_ips_data(ip_version=4)
generate_location_for_list_of_ips(ips)
```

此代码段在 “stats/location_data” 目录中生成 CAIDA 地理定位结果，文件名为 “caida_location_output_default”。

#### Maxmind 地理定位

要运行 Maxmind 地理定位，需要从 Maxmind 网站下载 Geolite-city.mmdb 文件，并将其放置在 “stats/location_data” 目录中。以下代码段用于在下载 mmdm 文件后获取 Maxmind 地理定位，函数详情见 “location/maxmind_geolocation_utils.py” 文件。

```python
links, ips = load_all_links_and_ips_data(ip_version=4)
generate_locations_for_list_of_ips(ips, ip_version=4)
```

此代码段在 “stats/location_data” 目录中生成 RIPE 地理定位结果，文件名为 “maxmind_location_output_v4_default”。

#### 其他地理定位

对于其余的地理定位，我们依赖于一个聚合网站。以下代码段可用于获取相关的 IP 位置，函数在 “location/ipgeolocation_utils.py” 文件中。

```python
args = {}
ip_version = 4
args['chromedriver_location'] = input('Enter chromedriver full path: ')
links, ips = load_all_links_and_ips_data(ip_version=4)
generate_location_for_list_of_ips(list_of_ips, in_chunks=False, args=args)
common_merge_operation('stats/location_data/iplocation_files', 2, [], ['ipgeolocation_file_'], True, f'iplocation_location_output_v{ip_version}_default')
```

初步生成的文件放置在 “stats/location_data/iplocation_files” 目录中。

### SoL 验证

完成所有地理定位计算后，需要对这些地理定位进行 SoL 验证。对于 IPv4，执行以下代码段。

```python
% 首先生成探针到坐标的映射，这是 SoL 验证所必需的
% 对于 RIPE，可以使用以下代码段（位于 ‘traceroute/ripe_probe_location_info.py’ 文件中）
load_probe_location_result()

% 对于 CAIDA，使用以下代码段（位于 ‘traceroute/caida_probe_location_info.py’ 文件中）
load_probe_to_coordinate_map()

% 重新运行 traceroute 以进行 SoL 验证
ripe_process_traceroutes(start_time, end_time, '5151', 4, True)
ripe_process_traceroutes(start_time, end_time, '5051', 4, True)
caida_process_traceroutes(2022, 3, 1647, 4, 1000, True)
common_merge_operation('stats/location_data', 0, [], ['validated_ip_locations'], True, 'all_validated_ip_location_v4')
```

SoL 验证后的地理定位信息将保存在 “stats/location_data/ all_validated_ip_location_v4”

注意：与初始 traceroute 生成步骤的唯一主要区别是将 ripe_process_traceroutes 和 caida_process_traceroutes 的最后一个参数设置为 True，这会触发 SoL 验证。如果在生成地理定位结果之前设置为 True，将会出现错误。

### IP 到 AS 的映射

要生成 IP 到 AS 的映射，可以使用 ip_to_as 目录中的文件。生成 IPv4 IP 到 AS 映射的代码段如下。

```python
links, ips = load_all_links_and_ips_data(ip_version=4)

% 对于 RPKI 查询，使用以下代码段（函数详情见 'ip_to_as/whois_rpki_utils.py' 文件）
args = {}
args['chromedriver_location'] = input('Enter chromedriver full path: ')
generate_ip2as_for_list_of_ips(ip_version=4, ips, args=args, in_chunks=False)

% 对于 RADB 查询，使用以下代码段（函数详情见 'ip_to_as/whois_radb_utils.py' 文件）
args = {}
args['whois_cmd_location'] = '/usr/bin/whois'
generate_ip2as_for_list_of_ips(ip_version=4, ips, args=args, in_chunks=False)

% 对于 Cymru whois 查询，使用以下代码段（函数详情见 'ip_to_as/cymru_whois_utils.py' 文件）
generate_ip2as_for_list_of_ips(ips, 4)
```

所有生成的 IP 到 AS 映射将保存在 ‘stats/ip2as_data’ 文件夹下。

注意：此外，对于 IPv4，需要从 CAIDA ITDK 下载相关的 CAIDA IP 到 AS 映射，并保存为 'stats/ip2as_data/caida_w

hois_output_v4_default'。

## Nautilus 映射

一旦生成了所有预备信息，实际的 Nautilus 映射就可以进行了。映射的代码段如下，相关文件可在 utils 目录中找到（主要是 'utils/common_utils.py'、'utils/geolocation_utils.py'、'utils/as_utils.py' 和 'utils/merge_utils.py'）。

```python
mode = 1
ip_version = 4

% 为每个类别生成初始映射
generate_cable_mapping(mode=mode, ip_version=ip_version, sol_threshold=0.05)

% 生成每个类别的最终映射文件
common_merge_operation('stats/mapping_outputs', 1, [], ['v4'], True, None)

% 合并所有类别的结果并重新更新类别映射
generate_final_mapping(mode=mode, ip_version=ip_version, threshold=0.05)
regenerate_categories_map(mode=mode, ip_version=ip_version)
```

如果先前的预处理步骤未正确完成，运行上述代码段时将显示标识缺失部分的相关错误消息。除了预处理步骤，还需要执行以下操作或下载（一次性操作）。

(i) 从 IPUMSI 下载国家形状文件（https://international.ipums.org/international/resources/gis/IPUMSI_world_release2020.zip）并将解压后的文件夹保存到 stats 目录中。
(ii) 使用以下命令执行 asrank.py 脚本：“python utils/asrank.py -v -a stats/asns.jsonl -o stats/organizations.jsonl -l stats/asnLinks.jsonl -u https://api.asrank.caida.org/v2/graphql”（此部分的所有代码和数据来自 CAIDA ASRank）。

最终映射结果将生成在 'stats' 目录中，文件名为 'link_to_cable_and_score_mapping_sol_validated_v4'（适用于 IPv4）和 'link_to_cable_and_score_mapping_sol_validated_v6'（适用于 IPv6）。

# 与先前工作的比较和验证

与先前工作和验证相关的文件可以在 'experiments' 和 'validation' 目录中找到。

## 与先前工作的比较

### iGDB

要运行 iGDB，首先必须从 https://github.com/standerson4/iGDB 下载代码库，并将两个文件（位于 experiments/iGDB/code 中）保存到 iGDB 的代码目录中（与当前保存的目录结构相似）。要比较使用默认地理定位的 iGDB，可以执行 'submarine_mapping.py' 文件；要比较使用 Nautilus 地理定位结果的 iGDB，可以执行 'submarine_mapping_with_nautilus_geolocation.py' 文件。

### Criticality-SCN

要与 SCN-Crit 进行比较，将使用 'experiments/scn_crit' 文件夹中的文件。我们已将原始 SCN-Crit 论文的结果保存为 'country_ip_sol_bundles.jsonl' 和 'country_ip_sol_bundles.jsonl.1'，并使用 '50_websites_mapping.py' 文件将映射结果与 Nautilus 进行比较。

注意：要运行 '50_websites_mapping.py' 文件，首先需要执行 'launch_ripe_traceroute.py' 文件，这需要 RIPE 积分来进行测量。因此，当脚本执行时，会提示输入 RIPE 密钥以执行测量。

## 验证

### 与过去的电缆故障比较

'validation/failure_analysis.py' 文件包括一个示例（论文中描述的也门电缆故障）。此文件可以修改以提供正确的结束日期、电缆和着陆点以计算故障前、期间和之后的链路数。该程序当前被编程为从 5051 和 5151 测量中下载对应于每个场景结束日期前两天的 traceroute 数据。

注意：默认情况下，每次运行文件都会自动开始下载对应日期的 RIPE traceroute。如果需要关闭此功能（例如针对同一故障时间段的第二次或第三次运行），可以通过将下载参数设置为 False 来关闭自动 RIPE traceroute 下载和处理（即，将代码从 get_ripe_data_for_given_end_date(date, True) 更改为 get_ripe_data_for_given_end_date(date, False)）。

### 目标 traceroute 测量

使用 'validation/loose_constraints_analysis.py' 文件进行目标 traceroute 测量。在执行该文件之前，需要执行 'validation/probe_search_and_initiate_traceroutes.py' 文件以生成相关 traceroute，然后使用 'validation/loose_constraints_analysis.py' 文件进行分析。

注意：这些测量将消耗大量 RIPE 积分，因此在运行上述脚本时需要谨慎。

注意：目标 traceroute 测量也依赖于 RIPE 积分，并会提示输入 RIPE 密钥和一个自定义术语（用于在测量执行后搜索测量结果）。

### 地理定位验证

为了评估地理定位映射的准确性，Nautilus 使用先前论文中生成的真实地理定位数据进行评估。要执行此操作，需要运行 'validation/geolocation_validation.py' 文件。