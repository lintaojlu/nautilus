from pathlib import Path

import pickle, time, sys

import warnings

from selenium.webdriver.chrome.service import Service

warnings.filterwarnings("ignore")

from selenium.webdriver.common.keys import Keys

from selenium import webdriver
from selenium.webdriver.common.by import By

from selenium.webdriver.chrome.options import Options

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

root_dir = Path(__file__).resolve().parents[2]


def save_rpki_whois_output(rpki_output, ip_version=4, tags='default'):
    save_directory = root_dir / 'stats/ip2as_data/'

    save_directory.mkdir(parents=True, exist_ok=True)

    save_file = save_directory / 'rpki_whois_output_v{}_{}'.format(ip_version, tags)

    with open(save_file, 'wb') as fp:
        pickle.dump(rpki_output, fp)


def generate_ip2as_for_list_of_ips(ip_version=4, list_of_ips=[], tags='default', args=None, in_chunks=True):
    rpki_output = {}
    url = 'https://rpki-validator.ripe.net/'

    chrome_options = Options()
    capabilities = DesiredCapabilities.CHROME
    capabilities["pageLoadStrategy"] = "eager"
    chrome_options.add_argument('--headless')
    chromedriver_location = args.get('chromedriver_location', None)
    chrome_service = Service(executable_path=chromedriver_location)


    if in_chunks:
        num_parallel = args.get('num_parallel', 5)
        max_ips_to_process = args.get('max_ips_to_process', None)
        chromedriver_binary = args.get('chromedriver_binary', None)
        chromedriver_location = args.get('chromedriver_location', None)

        chrome_options.add_argument('--no-sandbox')
        chrome_options.binary_location = chromedriver_binary

        all_ips_file = args.get('ips_file_location', None)

        with open(all_ips_file, 'rb') as fp:
            all_ips = pickle.load(fp)

        all_ips = list(all_ips)

        if num_parallel * max_ips_to_process > len(all_ips):
            list_of_ips = all_ips[(num_parallel - 1) * max_ips_to_process:]
        else:
            list_of_ips = all_ips[(num_parallel - 1) * max_ips_to_process: server_id * max_ips_to_process]

    else:
        if len(list_of_ips) == 0:
            print(f'Invalid list passed. Pass valid list of IPs')
            return None

    try:
        driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
        driver.get(url)

        for count, ip_address in enumerate(list_of_ips):

            query_box = driver.find_element(By.CLASS_NAME, 'el-input__inner')
            query_box.clear()
            query_box.send_keys(ip_address)

            button = driver.find_element(By.CLASS_NAME, 'el-button--primary')
            button.click()

            time.sleep(1)

            element = WebDriverWait(driver, 1).until(
                EC.presence_of_element_located((By.CLASS_NAME, "validation-header")))

            while True:
                try:
                    content = driver.find_elements(By.CLASS_NAME, 'mono')
                    result = [item.text for item in content if 'AS' in item.text]
                    print(f'IP address is {ip_address} and result is {result}')
                    break
                except:
                    print('Retrying again')
                    time.sleep(0.5)

            if len(result) > 0:
                rpki_output[ip_address] = result

            if count % 100 == 0:
                print(f'Doing a partial save at count {count}')
                save_rpki_whois_output(rpki_output, ip_version, tags)

        driver.close()
        driver.quit()

        print(f'Doing a final save with {len(rpki_output)} IPs processed')
        save_rpki_whois_output(rpki_output, ip_version, tags)

    except Exception as e:
        print(f'We got an error: {str(e)}')
        return None

    return rpki_output


def load_rpki_whois_output(args, in_chunks, ips_list=None, tags='default'):
    if ips_list is None:
        ips_list = []
    file_location = root_dir / 'stats/ip2as_data/rpki_whois_output_{}'.format(tags)

    if Path(file_location).exists():
        with open(file_location, 'rb') as fp:
            rpki_output = pickle.load(fp)
    else:
        if len(ips_list) > 0:
            rpki_output = generate_ip2as_for_list_of_ips(ips_list, tags, args, in_chunks=in_chunks)
        else:
            print(f'Please enter either valid file tag or ips list')
            return None

    return rpki_output


if __name__ == '__main__':

    mode = 1
    args = {}
    list_of_ips = []

    ip_version = 4

    if mode == 0:
        args['num_parallel'] = int(sys.argv[1])
        args['max_ips_to_process'] = int(sys.argv[2])
        ip_version = sys.argv[2]
        args['ips_file_location'] = root_dir / 'stats/mapping_outputs/all_ips_v{}'.format(ip_version)
        args['chromedriver_binary'] = input('Enter chromedriver binary full path: ')
        args['chromedriver_location'] = root_dir / 'chromedriver'
    else:
        args['chromedriver_location'] = root_dir / 'chromedriver'

        with open(root_dir / f'stats/mapping_outputs/all_ips_v{ip_version}', 'rb') as fp:
            list_of_ips = pickle.load(fp)
    # list_of_ips = ['66.85.82.9', '156.225.182.1', '67.59.254.241', '103.78.227.1', '193.34.197.140', '23.111.226.1', '193.0.214.1', '152.255.147.235', '216.19.218.1']

    if mode == 0:
        in_chunks = True
    else:
        in_chunks = False

    rpki_output = generate_ip2as_for_list_of_ips(ip_version, list_of_ips, args=args, in_chunks=in_chunks)

    print(f'We got results for {len(rpki_output)} IPs')
