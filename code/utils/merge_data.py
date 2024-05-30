import os
import pickle
import sys
from pathlib import Path

import numpy as np

sys.path.insert(1, os.path.abspath('../../'))
root_dir = Path(__file__).resolve().parents[2]


def check_file_presence(file_to_be_checked, list_of_files, keywords):
    if len(list_of_files) > 0:
        return any([True for file in list_of_files if file in str(file_to_be_checked)])

    if len(keywords) > 0:
        return any([True for keyword in keywords if keyword in str(file_to_be_checked)])

    return False


def save_results_to_file(result, directory, save_file_name):
    print('Saving the file: {}/{}'.format(directory, save_file_name))
    with open('{}/{}'.format(directory, save_file_name), 'wb') as fp:
        pickle.dump(result, fp)


def merge_sol_testing_results(directory, list_of_files=[], keywords=[]):
    final_result = {}

    files = Path(directory).glob('*')

    for file in files:
        if check_file_presence(file, list_of_files, keywords):
            print(f'Currently processing {str(file)}')
            with open(file, 'rb') as fp:
                file_contents = pickle.load(fp)

            # Updating the final result
            common_keys = list(set(final_result.keys()) & set(file_contents.keys()))

            replacement = {}

            for key in common_keys:
                existing_result = final_result[key]
                current_result = file_contents[key]

                # Updating the penalty
                new_penalty = np.add(existing_result['penalty_count'], current_result['penalty_count']).tolist()
                new_count = np.add(existing_result['total_count'], current_result['total_count']).tolist()

                replacement[key] = {'location_index': existing_result['location_index'],
                                    'coordinates': existing_result['coordinates'],
                                    'penalty_count': new_penalty,
                                    'total_count': new_count}

            # First let's update the file contents, but this will overwrite the value for common keys
            final_result.update(file_contents)

            # This update will take care of the common keys
            final_result.update(replacement)

            print(
                f'Final result length is {len(final_result)} and current processed file length is {len(file_contents)}')

    return final_result


def merge_cable_mapping_results_for_each_category(directory, list_of_files=[], keywords=[], save_results=True,
                                                  limit=1000):
    # keyword 0 should be v4 or v6

    categories = ['bg_oc', 'og_oc', 'bb_oc', 'bg_te', 'og_te', 'bb_te']

    print(f'Limit is {limit}')

    for category in categories:

        files = Path(directory).glob('*')

        mapping = {}
        mapping_sol_validated = {}

        count = 0

        for file in files:
            if '{}_{}'.format(category, keywords[0]) in str(file):
                print(f'Currently processing file {str(file)}')
                if 'sol_validated' in str(file):
                    with open(file, 'rb') as fp:
                        content = pickle.load(fp)
                    mapping_sol_validated.update(content)
                    count += 1
                    print(f'Processed {count} files')
                else:
                    with open(file, 'rb') as fp:
                        content = pickle.load(fp)
                    mapping.update(content)
                    count += 1
                    print(f'Processed {count} files')

                if count >= limit:
                    break

        print(f'For category {category}, we have {len(mapping)} and {len(mapping_sol_validated)} results respectively')

        if save_results:
            save_results_to_file(mapping, str(directory), 'cable_mapping_{}_{}_merged'.format(category, keywords[0]))
            save_results_to_file(mapping_sol_validated, str(directory),
                                 'cable_mapping_sol_validated_{}_{}_merged'.format(category, keywords[0]))


def merge_iplocation_net_results(directory, list_of_files=[], keywords=[], save_results=True, save_file_name=None):
    iplocation_merged_result = {}
    files = Path(directory).glob('*')

    for file in files:
        if keywords[0] in str(file):
            print(f'Processing {file} now')
            with open(file, 'rb') as fp:
                file_contents = pickle.load(fp)

            iplocation_merged_result.update(file_contents)

    if len(iplocation_merged_result) > 0 and save_results:
        if save_file_name == None:
            save_file_name = directory + '/../iplocation_location_output_v4_default'
        else:
            save_file_name = directory + '/../' + save_file_name

        with open(save_file_name, 'wb') as fp:
            pickle.dump(iplocation_merged_result, fp)


def merge_caida_uniq_dicts(directory, list_of_files=[], keywords=[], save_results=True, save_file_name=None):
    uniq_caida_merged_result = {}
    files = Path(directory).glob('*')

    for file in files:
        if keywords[0] in str(file) and 'merged' not in str(file):
            print(f'Processing {file} now')
            with open(file, 'rb') as fp:
                file_contents = pickle.load(fp)
            print(f'Finished loading the file')

            overlap = list(set(uniq_caida_merged_result) & set(file_contents))

            overlap_dict = {}

            for item in overlap:
                overlap_dict[item] = uniq_caida_merged_result[item] + file_contents[item]

            uniq_caida_merged_result.update(file_contents)
            uniq_caida_merged_result.update(overlap_dict)

    print(f'Starting write to file, content length: {len(uniq_caida_merged_result)}')

    if len(uniq_caida_merged_result) > 0 and save_results:
        if save_file_name == None:
            save_file_name = directory + '/uniq_ip_dict_caida_all_links_v4_merged'
        else:
            save_file_name = directory + save_file_name

        print(f'Saving at {save_file_name}')

        with open(save_file_name, 'wb') as fp:
            pickle.dump(uniq_caida_merged_result, fp)


def common_merge_operation(directory, operation_code, list_of_files=[], keywords=[], save_results=True,
                           save_file_name=None, limit=1000):
    if Path(directory).exists():
        if (len(list_of_files) > 0 and len(keywords) > 0) or (len(list_of_files) == 0 and len(keywords) == 0):
            print(f'Pass in either list of files or keywords, but not both and should be either one')
            return None
        else:
            # 0 for SoL testing merge operations
            if operation_code == 0:
                final_result = merge_sol_testing_results(directory, list_of_files, keywords)
                if save_results and save_file_name:
                    save_results_to_file(final_result, directory, save_file_name)
                    return final_result
            # 1 for cable matching merge operations
            elif operation_code == 1:
                merge_cable_mapping_results_for_each_category(directory, list_of_files, keywords, limit=limit)
            # 2 for iplocation merge operations
            elif operation_code == 2:
                merge_iplocation_net_results(directory, list_of_files, keywords, save_results, save_file_name)
            elif operation_code == 3:
                merge_caida_uniq_dicts(directory, list_of_files, keywords, save_results, save_file_name)

    else:
        print('Given directory does not exist, pass the correct directory')
        return None


if __name__ == '__main__':
    # common_merge_operation('stats/location_data', 0, [], ['validated_ip_locations'], True, 'all_validated_ip_location_v4')
    common_merge_operation(root_dir / 'stats/mapping_outputs', 1, [], ['v4'], True, None)
# common_merge_operation('stats/location_data/iplocation_files', 2, [], ['ipgeolocation_file_v4_'], True, 'iplocation_location_output_v4_default')
# common_merge_operation('stats/caida_data', 3, [], ['uniq_ip_dict_caida_all_links_v6_'], True, 'uniq_ip_dict_caida_all_links_v6_merged')
