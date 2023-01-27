# CS 121 Project 2
**frontier.py**: This file acts as a representation of a frontier. It has method to add a URL to the frontier, get the next URL and check if the frontier has any more URLs. Additionally, it has methods to save the current state of the frontier and load existing state

**crawler.py**: This file is responsible for scraping URLs from the next available link in frontier and adding the scraped links back to the frontier

**corpus.py**: This file is responsible for handling corpus related functionalities like mapping a URL to its local file name and fetching a file meta-data and content from corpus. In order to make it possible to work on a crawler without accessing the ICS network, this file accesses a static corpus and maps given URLs to local file names that contain the content of that URL.

**main.py**: This file glues everything together and is the starting point of the program. It instantiates the frontier and the crawler and starts the crawling process. It also registers a shutdown hook to save the current frontier state in case of an error or receiving of a shutdown signal.
