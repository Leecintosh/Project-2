import pickle
import os
from urllib.parse import urlparse
from collections import defaultdict

FRONTIER_DIR_NAME = "frontier_state"
URL_SET_FILE_NAME = os.path.join(".", FRONTIER_DIR_NAME, "url_set.pkl")

subdomains = defaultdict(int)

def get_subdomain(domain : str):
    domain_list = domain.split('.')[:-2]
    return '.'.join(domain_list)


def print_dict(dictionary : dict):
    for key in dictionary:
        print(f'{key}\t\t:{dictionary[key]}')


if __name__ == '__main__':

    urls_set = pickle.load(open(URL_SET_FILE_NAME, "rb"))

    counter = 0

    for url in urls_set:
        url_parsed = urlparse(url)
        subdomain = get_subdomain(url_parsed.netloc)
        subdomains[subdomain] += 1

    print_dict(subdomains)
