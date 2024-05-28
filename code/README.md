# Generating Nautilus Mapping

The Nautilus mapping is split into (i) Pre-processing and (ii) the actual mapping stages. The reason for this is to ensure efficiency due to large scale of measurements (ie., in the order of millions) that might need to be carried out.

Note: All the files need to be run from the codebase root directory (ie., nautilus/code/ directory) to ensure all intermediate file representations are accessed correctly.

## Pre-processing steps

### Generating the traceroutes

Nautilus relies on 2 sources for its traceroutes: (i) RIPE Atlas — 5051 and 5151 for IPv4 and 6052 and 6152 for IPv6, and (ii) CAIDA — /24 and /48 prefix probing for IPv4 and IPv6 respectively

#### RIPE Atlas traceroute generation

To generate the traceroutes within a given time frame, the following code segment should be used. Here is an example for code for 5151 measurement id between 15th and 29th March 2022. The relevant functions are present in “traceroute/ripe_traceroute_utils.py” file

```
start_time = datatime(2022, 3, 15, 0)
end_time = datetime(2022, 3, 29, 0)
ripe_process_traceroutes(start_time, end_time, '5151', 4, False)
```


In the above function, 4 indicates the ip version and for traceroute collection initially the last parameter should be set to False. The result of this operation will be saved as a file “stats/ripe_data/uniq_ip_dict_5151_…..”

Note: Multiple files will be generated from the prior operation, but what we require is only the final file generated which will typically have a “<date>_count_” towards the end as it’s file name. For instance, the above operation will generate “uniq_ip_dict_5151_all_links_v4_min_all_latencies_only_03_29_2022_00_00_count_28” as the final file that will be used for further processing.

#### CAIDA traceroute generation

To generate traceroutes from CAIDA, instead of the timeframe, the corresponding cycle id for the specific duration would be required. For instance for the timeframe between March 13-23 in 2022, the corresponding cycle id is 1647. Hence to generate the traceroutes for this example, the following code segment is used. The relevant functions are present in “traceroute/caida_traceroute_utils.py” file

`caida_process_traceroutes(2022, 3, 1647, 4, 1000, False) `

1647 corresponds to the cycle id, 2022 and 3 are the corresponding year and month for the cycle, 4 refers to the IP version and the last two parameters can be left at the default values for the initial traceroute generation phase.

Note: To run the following code segment, the “scamper” tool from CAIDA needs to be installed to process the warts file downloaded from CAIDA servers. The final generated file will be present in “stats/caida_data/uniq_ip_dict_caida_all_links_v4_min_all_latencies_only_….”

Note: For running /24 probing with CAIDA (IPv4), access will be required and the code will prompt for the username and password, which will need to be provided for the program to execute and process CAIDA traceroutes. For IPv6, since the data is public, if measurement id is provided, Nautilus automatically pulls and processes the required traceroutes.

### Generating all the unique IPs and links

Finally once the traceroutes are generated from RIPE and CAIDA, we need to get all the relevant IP endpoints and links. The code snippet for this is shown below and the function is based in “utils/traceroute_utils.py” file.

`load_all_links_and_ips_data(ip_version=4)`

This generates “all_ips_v4” and “links_v4” in the “stats/mapping_outputs” directory which correspond to the list of unique IPs and links respectively

### Generating geolocation results

#### RIPE geolocation 

First to run the geolocation script, the essential RIPE geolocation files need to be downloaded from the RIPE ftp server and places in “stats/location_data” directory. The code snippet for this is shown below and the details of this function can be found at “location/ripe_geolocation_utils.py” file

```
links, ips = load_all_links_and_ips_data (ip_version=4)
generate_location_for_list_of_ips_ripe(ips, ip_version=4)
```


This code snippet generates the RIPE geolocation results as “ripe_location_output_v4_default” in the “stats/location_data” directory.

#### CAIDA geolocation

Similarly for CAIDA, the midar.iff and midar.iff.nodes.geo files should be downloaded from the CAIDA Ark platform and placed in “stats/location_data” directory. The code snippet to get the CAIDA geolocation for a list of IPs is shown below and the function details can be found in “location/caida_geolocation_utils.py” file

```
links, ips = load_all_links_and_ips_data (ip_version=4)
generate_location_for_list_of_ips(ips)
```


This code snippet generates the CAIDA geolocation results as “caida_location_output_default” in the “stats/location_data” directory.

#### Maxmind geolocation

For maxmind geolocation to run, the Geolite-city.mmdb file needs to be downloaded from Maxmind website and placed in the “stats/location_data” directory. The following code snippet is to get the Maxmind geolocation post this mmdm file download and the the function details can be found in “location/maxmind_geolocation_utils.py” file

```
links, ips = load_all_links_and_ips_data (ip_version=4)
generate_locations_for_list_of_ips(ips, ip_version=4)
```


This code snippet generates the RIPE geolocation results as “maxmind_location_output_v4_default” in the “stats/location_data” directory.

#### Other geolocation

For the rest of geolocation, we rely on an aggregator website. The following code snippet can be used to get the relevant ip locations and the corresponding functions are in “location/ipgeolocation_utils.py”

```
args = {}
ip_version = 4
args['chromedriver_location'] = input('Enter chromedriver full path: ')
links, ips = load_all_links_and_ips_data (ip_version=4)
generate_location_for_list_of_ips (list_of_ips, in_chunks=False, args=args)
common_merge_operation('stats/location_data/iplocation_files', 2, [], ['ipgeolocation_file_'], True, f’iplocation_location_output_v{ip_version}_default') % This operation is from the 'utils/merge_utils.py' file
```


The initial generated files are placed in the “stats/location_data/iplocation_files” directory

### SoL validation

Once all geolocation computations are completed, these geolocations need to be SoL validated. For IPv4, the following code snippet needs to be executed

```
% First generating the probe to coordinate mappings which are essential for SoL validation
% For RIPE, the following code snippet can be used (found in ‘traceroute/ripe_probe_location_info.py’ file)
load_probe_location_result() 

% For CAIDA, use the following (found in ‘traceroute/caida_probe_location_info.py’ file)
load_probe_to_coordinate_map()

% Re-running traceroutes to perform SoL validation
ripe_process_traceroutes(start_time, end_time, '5151', 4, True)
ripe_process_traceroutes(start_time, end_time, '5051', 4, True)
caida_process_traceroutes(2022, 3, 1647, 4, 1000, True)
common_merge_operation('stats/location_data', 0, [], ['validated_ip_locations'], True, 'all_validated_ip_location_v4') % This operation is from the 'utils/merge_utils.py' file
```


The SoL validated geolocation information will be saved at “stats/location_data/ all_validated_ip_location_v4”

Note: The only major difference with the initial traceroute generation step is the last parameter being set to True for ripe_process_traceroutes and caida_process_traceroutes, which triggers SoL validation. If this is set to True prior to generation of geolocation results, an error will be observed.

### IP to AS mapping

To generate the IP to AS mapping, the files in ip_to_as directory can be used. A code snippet for generating the IP to AS maps for IPv4 is shown below

```
links, ips = load_all_links_and_ips_data (ip_version=4)

% For RPKI queries use the following (function details in 'ip_to_as/whois_rpki_utils.py')
args = {}
args['chromedriver_location'] = input('Enter chromedriver full path: ')
generate_ip2as_for_list_of_ips(ip_version=4, ips, args=args, in_chunks=False)

% For RADB querying use (function details in 'ip_to_as/whois_radb_utils.py')
args = {}
args['whois_cmd_location'] = '/usr/bin/whois'
generate_ip2as_for_list_of_ips(ip_version=4, ips, args=args, in_chunks=False)

% For Cymru whois queries, use (function details in 'ip_to_as/cymru_whois_utils.py')
generate_ip2as_for_list_of_ips(ips, 4)
```


All the generated IP to AS maps will be saved under ‘stats/ip2as_data’ folder

Note: Additionally for IPv4, the relevant CAIDA IP to AS mapping needs to be downloaded from CAIDA ITDK and saved as 'stats/ip2as_data/caida_whois_output_v4_default'

## Nautilus Mapping

Once all the pre-requisite information has been generated, the actual Nautilus mapping can take place. The code snippets for the mapping is shown below and associated files with this stage can be found in the utils directory (predominantly 'utils/common_utils.py', 'utils/geolocation_utils.py', 'utils/as_utils.py' and 'utils/merge_utils.py')

```
mode = 1
ip_version = 4

% Generate an initial mapping for each category
generate_cable_mapping(mode=mode, ip_version=ip_version, sol_threshold=0.05)

% Generating a final mapping file for each category
common_merge_operation('stats/mapping_outputs', 1, [], ['v4'], True, None)

% Merging the results for all categories and re-updating the categories map
generate_final_mapping(mode=mode, ip_version=ip_version, threshold=0.05)
regenerate_categories_map (mode=mode, ip_version=ip_version)
```


If the prior pre-processing steps are not completed properly, the relevant error message identifying the missing pieces will be displayed while running the above code snippet. In addition to the pre-processing steps, the following operations or downloads will be needed to be carried out (one-time operation)
(i) Download a countries shape file from IPUMSI (https://international.ipums.org/international/resources/gis/IPUMSI_world_release2024.zip) and the unzipped folder needs to be saved in stats directory 
(ii) Execute asrank.py script using the following command “python utils/asrank.py -v -a stats/asns.jsonl -o stats/organizations.jsonl -l stats/asnLinks.jsonl -u https://api.asrank.caida.org/v2/graphql” (All the code and the data is from CAIDA ASRank for this portion)

The final mapping results will be generated in the 'stats' directory as 'link_to_cable_and_score_mapping_sol_validated_v4' for IPv4 and 'link_to_cable_and_score_mapping_sol_validated_v6' for IPv6 

# Comparison to prior works and Validation 

The files corresponding to prior works and validation can be found within the 'experiments' and 'validation' directories respectively.

## Comparison to prior works

### iGDB

To run iGDB, first the codebase must be downloaded from https://github.com/standerson4/iGDB and the two files (within experiments/iGDB/code) should be saved within the code directory of iGDB (similar directory structure as currently saved). To compare iGDB with their default geolocation, the 'submarine_mapping.py' file can be executed and for comparing iGDB using the geolocation results from Nautilus, the 'submarine_mapping_with_nautilus_geolocation.py' file can be executed 

### Criticality-SCN 

To compare with SCN-Crit, the files with 'experiments/scn_crit' folder will be used. We have already saved the results from the original SCN-Crit paper as 'country_ip_sol_bundles.jsonl' and 'country_ip_sol_bundles.jsonl.1' and we compare the mapping results with Nautilus using the '50_websites_mapping.py' file.

Note: To run the 50_websites_mapping.py file, first the launch_ripe_traceroute.py file needs to be executed, which will need RIPE credits to carry out the measurements. Hence when the script is executed, an input prompt would request for the RIPE key to carry out the measurements.

## Validation 

### Comparing with past cable failures

The 'validation/failure_analysis.py' file includes an example (of the Yemen cable failure described in the paper). This file can be modified to provide the correct end-dates, cable and landing points failed to compute the number of links present before, during and after the failure. This program is currently programmed to download 2 days worth of traceroute data from both 5051 and 5151 measurements corresponding to the 2 days prior to the end-date for each scenario.

Note: The file by default everytime automatically start downloading the RIPE traceroutes corresponding to the provided dates. If this needs to be turned off (which might be case for say a 2nd or 3rd run for the same failure timeframe), the automatic RIPE traceroute download and processing can be turned off by setting the download parameter to False (ie., changing the code from get_ripe_data_for_given_end_date(date, True) to get_ripe_data_for_given_end_date(date, False))

### Targeted traceroute measurements

The targeted traceroute measurements are carried out using 'validation/loose_constraints_analysis.py' file. Before executing the prior file, 'validation/probe_search_and_initiate_traceroutes.py' file will need to be executed to generate the relevant traceroutes, which are analysed using 'validation/loose_constraints_analysis.py' file.

Note: These measurements will consume a large amount of RIPE credits, so caution needs to exercised while running the above script.

Note: The targeted traceroute measurements also rely on RIPE credits and will prompt for the RIPE key and a custom term (to search for measurements once executed). 

### Geolocation validation 

To evaluate the accuracy of geolocation mappings, Nautilus was evaluated against the ground-truth geolocation data generated in a prior paper. To perform this, the file 'validation/geolocation_validation.py' needs to be executed.
