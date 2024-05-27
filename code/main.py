from collections import namedtuple
from datetime import datetime

from traceroute import ripe_traceroute_utils

Location = namedtuple('Location', ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude',
                                   'autonomous_system_number', 'network', 'ISP', 'Org'])
MaxmindLocation = namedtuple('MaxmindLocation',
                             ['city', 'subdivisions', 'country', 'accuracy_radius', 'latitude', 'longitude',
                              'autonomous_system_number', 'network'])

if __name__ == '__main__':
    # prepare traceroutes
    start_time = datetime(2024, 5, 1, 0)
    end_time = datetime(2024, 5, 2, 0)

    # The measurement ids used are 5051 and 5151 for v4 and 6052 and 6152 for v6
    # The number (6) in example below indicates the IP version, which will be according to the measurement IDs
    # That number is mostly used for just saving the results

    result = ripe_traceroute_utils.ripe_process_traceroutes(start_time, end_time, '5051', 4, True)
    print(f'Result length is {len(result)}')
    result = ripe_traceroute_utils.ripe_process_traceroutes(start_time, end_time, '5151', 4, True)
    print(f'Result length is {len(result)}')
