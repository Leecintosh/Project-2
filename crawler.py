import logging
import re
from urllib.parse import urlparse

from lxml import etree, html
from urllib.parse import urljoin
import time
import urllib.parse
logger = logging.getLogger(__name__)


class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus
        self.stats = Statistic()

    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        outputLinks = []
        url = url_data["url"]
        content = url_data["content"]
        if not content or not self.is_valid(url):
            return []
        if content[0:5] == '<?xml':
            content = content[content.find('>') + 1: -1]
        parser = html.HTMLParser(recover=True, encoding="utf-8")
        try:
            root = html.fromstring(content, parser=parser)
            links = root.xpath("//a/@href")
            for link in links:
                joined_url = urljoin(url, link)
                if not self.is_valid(joined_url):
                    self.stats.add_trap(joined_url)
                else:
                    self.stats.add_downloaded_url(joined_url)
                    self.stats.add_subdomain(joined_url)
                    outputLinks.append(joined_url)
            self.stats.add_longest_page(url, self.stats.count_words(root))
            self.stats.add_all_words(root)
        except etree.ParserError:
            pass
        self.stats.record_page_valid(url, len(outputLinks))
        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """

        parsed = urlparse(url)
        '''
        check history trap
        '''
        history_trap = ["time", "timestamp", "page"]
        for i in history_trap:
            if i in parsed.query:
                return False
        '''
        http://example.com/search?q=dynamic+urls&id=abcd1234
        or "&" in parsed.query
        or "=" in parsed.query or
        or "&" in parsed.query
        '''
        if "=" in parsed.query or '+' in parsed.query:
            return False

        if parsed.path is not None:
            temp = parsed.path.split('/')
            subdirectory = [subdir for subdir in temp if subdir]
            if len(subdirectory) != 0:
                for i in range(len(subdirectory) // 2):
                    if subdirectory[i] != subdirectory[-i - 1]:
                        break
                else:
                    return False

        if parsed.fragment is None:
            return False

        if parsed.scheme not in set(["http", "https"]):
            return False

        if len(url) > 2000:
            return False

        try:
            return ".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())

        except TypeError:
            print("TypeError for ", parsed)
            return False


class Statistic:
    def __init__(self):
        self.subdomains = dict()
        self.page_valid = ['', -1]
        self.downloaded_url = set()
        self.traps = set()
        self.longest_page = ['', -1]
        self.all_words = []

    def record_page_valid(self, page : str, number : int):
        if number > self.page_valid[1]:
            self.page_valid[0] = page
            self.page_valid[1] = number

    def add_downloaded_url(self, url : str):
        self.downloaded_url.add(url)

    def add_trap(self, url : str):
        self.traps.add(url)

    def add_subdomain(self, url : str):
        subdomain = urlparse(url).netloc
        if subdomain in self.subdomains:
            self.subdomains[subdomain].add(url)
        else:
            self.subdomains[subdomain] = {url}

    def count_words(self, parsed_content):
        return len(parsed_content.text_content().split())

    def add_longest_page(self, url : str, count : int):
        if count > self.longest_page[1]:
            self.longest_page[0] = url
            self.longest_page[1] = count

    def add_all_words(self, parsed_content):
        text = parsed_content.text_content().lower()
        tokens = []
        word = ""
        for char in text:
            if char.isalpha():
                word += char
            else:
                if len(word) > 1:
                    tokens.append(word)
                    word = ""
        if len(word) > 1:
            tokens.append(word)
        self.all_words.extend(tokens)

    def save(self, path : str = "analysis.txt"):
        # question 1
        with open(path, 'w', encoding='utf-8') as f:
            f.write("1. Keep track of the subdomains that it visited, and count how many different URLs it has processed from each of those subdomains\n")
            for subdomain in self.subdomains:
                f.write(f'\t{subdomain} {len(self.subdomains[subdomain])}\n')

        # question 2
        with open(path, 'a', encoding='utf-8') as f:
            f.write("\n2. Find the page with the most valid out links (of all pages given to your crawler). Out Links are the number of links that are present on a particular webpage\n")
            f.write(f'the page with the most valid out links: {self.page_valid[0]}\n')

        # question 3
        with open(path, 'a', encoding='utf-8') as f:
            f.write("\n3. List of downloaded URLs and identified traps\n")
            f.write(f'the list of downloaded links:\n')
            counter = 0
            for url in self.downloaded_url:
                f.write(f'\t{url}\n')
                if counter == 1000:
                    f.flush()
                    counter = 0

            counter = 0
            f.write(f'the list of identified traps:\n')
            for trap in self.traps:
                f.write(f'\t{trap}\n')
                if counter == 1000:
                    f.flush()
                    counter = 0

        # question 4
        with open(path, 'a', encoding='utf-8') as f:
            f.write("\n4. What is the longest page in terms of number of words? (HTML markup doesn’t count as words)\n")
            f.write(f'\nthe longest page: {self.longest_page[0]} with {self.longest_page[1]} words\n')


        # question 5
        with open(path, 'a', encoding='utf-8') as f:
            f.write("\n5. What are the 50 most common words in the entire set of pages? (Ignore English stop words, which can be found, (https://www.ranks.nl/stopwords)\n")
            stop_words = {'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', "aren't", 'as', 'at', 'be', 'because',
                          'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do',
                          'does', "doesn't", 'doing', "don't", 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't",
                          'have', "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 'his',
                          'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 'itself', "let's", 'me',
                          'more', 'most', "mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our',
                          'ours', 'ourselves', 'out', 'over', 'own', 'same', "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't", 'so', 'some',
                          'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', "there's", 'these', 'they', "they'd",
                          "they'll", "they're", "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', "wasn't", 'we', "we'd",
                          "we'll", "we're", "we've", 'were', "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's",
                          'whom', 'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves'}
            word_freq = {}
            for word in self.all_words:
                if word in stop_words:
                    continue
                if word not in word_freq:
                    word_freq[word] = 1
                else:
                    word_freq[word] += 1

            sorted_word_freq = [(k, v) for k, v in sorted(word_freq.items(), key=lambda item: item[1], reverse=True)]
            most_common_words = sorted_word_freq[:50]
            for word, freq in most_common_words:
                f.write(f'\t{word} {freq}\n')