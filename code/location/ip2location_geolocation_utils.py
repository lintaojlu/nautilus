from pathlib import Path
import IP2Location
from tqdm import tqdm
from collections import namedtuple

root_dir = Path(__file__).resolve().parents[2]

Location = namedtuple('Location', ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude',
                                   'autonomous_system_number', 'network', 'ISP', 'Org'])


def locate_ips_by_ip2location(ips):
    # Initialize IP2Location object with the path to your BIN file
    database = IP2Location.IP2Location(root_dir / "stats/location_data/IP2LOCATION-LITE-DB11.BIN")

    # Dictionary to store results
    ip_results = {}

    # Loop over each IP in the list
    for ip in tqdm(ips, desc='ip2location'):
        record = database.get_all(ip)
        # Check if the query was successful
        if record:
            # Create a Location namedtuple with the retrieved data
            location_data = Location(city=record.city,
                                     subdivisions=record.region,
                                     country=record.country_short if record.country_short != '-' else '',
                                     accuracy_radius='',
                                     latitude=float(record.latitude) if not record.latitude.startswith('0.0') else None,
                                     longitude=float(record.longitude) if not record.longitude.startswith(
                                         '0.0') else None,
                                     autonomous_system_number=record.asn,
                                     network=record.domain,
                                     ISP=record.isp,
                                     Org=record.as_name)

            # Store the Location namedtuple in the dictionary
            ip_results[ip] = location_data
        else:
            # If no record found, store None for this IP
            ip_results[ip] = None

    # Close the database
    database.close()

    return ip_results
