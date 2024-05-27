from pathlib import Path

import pickle

from collections import namedtuple

import geoip2.database
root_path = Path(__file__).parent.parent.parent

MaxmindLocation = namedtuple('MaxmindLocation', ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude', 'autonomous_system_number', 'network'])


def save_maxmind_output(maxmind_location, ip_version=4, tags='default'):

	with open(root_path / 'stats/location_data/maxmind_location_output_v{}_{}'.format(ip_version, tags), 'wb') as fp:
		pickle.dump(maxmind_location, fp)


def generate_locations_for_list_of_ips (ips_list, ip_version=4, tags='default'):

	maxmind_location = {}

	skipped_ips = []

	mmdb_file = root_path / 'stats/location_data/GeoLite2-City.mmdb'

	# Checking presence of mmdb file
	if Path(mmdb_file).exists():
		with geoip2.database.Reader(mmdb_file) as reader:
			for count, ip_address in enumerate(ips_list):

				try:
					response = reader.city(ip_address)
				except:
					skipped_ips.append(ip_address)
					continue

				try:
					city = response.city.names['en']
				except:
					city = None

				try:
					subdivisions = response.subdivisions[0].names['en']
				except:
					subdivisions = None

				try:
					country = response.country.names['en']
				except:
					country = None

				location = MaxmindLocation(city, subdivisions, country,
											response.location.accuracy_radius, 
											response.location.latitude, 
											response.location.longitude, 
											response.traits.autonomous_system_number, 
											response.traits.network)

				maxmind_location[ip_address] = location

		save_maxmind_output(maxmind_location, ip_version)

		return (maxmind_location, skipped_ips)

	else:
		print (f'File not found. The mmdb file should be downloaded from Maxmind city database and saved as stats/location_data/GeoLite2-City.mmdb')
		return (None, None)


def load_maxmind_output(ip_version=4, tags='default', ips_list=[]):


	file_location = root_path / 'stats/location_data/maxmind_location_ouput_v{}_{}'.format(ip_version, tags)

	if Path(file_location).exists():
		with open(file_location, 'rb') as fp:
			maxmind_location = pickle.load(fp)

	else:
		if len(ips_list) > 0:
			maxmind_location, _ = generate_locations_for_list_of_ips(ips_list)
		else:
			print (f'Please enter either valid file tag or ips list')
			return None

	return maxmind_location


if __name__ == '__main__':

	# sample_ips_list = ['66.85.82.9', '156.225.182.1', '67.59.254.241', '103.78.227.1', '193.34.197.140', '23.111.226.1', '193.0.214.1', '152.255.147.235', '216.19.218.1']
	ip_version = 4

	with open(root_path / f'stats/mapping_outputs/all_ips_v{ip_version}', 'rb') as fp:
		sample_ips_list = pickle.load(fp)

	maxmind_location, skipped_ips = generate_locations_for_list_of_ips(sample_ips_list, ip_version)

	print (f'We got results for {len(maxmind_location)} IPs and missed {len(skipped_ips)} IPs')
