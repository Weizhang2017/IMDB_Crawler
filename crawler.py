from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
# from pymongo import MongoClient
import logging
from time import sleep
import os

logging.basicConfig(level=logging.INFO)

class PyMongo:
    def __init__(self):
        self.col_raw = MongoClient('localhost', 27017).IMDB.raw

    def insert(self, data):
        self.col_raw.insert(data)

    def check_deplicate(self, url):
        return self.col_raw.find_one({'url': url})

class Crawl:

    BASE_URL = 'https://www.imdb.com'

    def __init__(self):
        self.urls = {self.BASE_URL}

    def get_data(self, url):
        html = urlopen(url).read()
        self.bsObj = BeautifulSoup(html, 'html.parser')
        title = self.bsObj.title.string
        data = {
                'url': url,
                'title': title,
                'raw': html,
        }
        return data

    def get_url(self):
        for link in self.bsObj.findAll('a', href=re.compile('^(/title/)((?!:).).*(/?ref_=)+.*$')):
            if 'href' in link.attrs:
                self.urls.add('{}{}'.format(self.BASE_URL, link.attrs['href']))

    def start(self):
        logging.info('start')
        while self.urls:
            _url = self.urls.pop()
            logging.debug(f'getting data from url {_url}')
            try: 
                data = self.get_data(_url)
            except Exception as e:
                logging.debug(f'error getting data: {str(e)}')
            else:
                logging.debug(f'get data for title {data["title"]}')
                try:
                    self.get_url()
                except Exception as e:
                    logging.error(f'error getting urls: {str(e)}')
                logging.debug(f'urls to be scraped {self.urls}')
                try:
                    self.save_to_file(data)
                except Exception as e:
                    logging.error(f'error saving to file: {str(e)}')
                sleep(20)

    def save_to_file(self, data):
        folder = os.environ['DIR']
        title = data['title']
        with open(f'{folder}/{title}', 'wb') as f:
            f.write(data['raw'])


def save_to_file():
    base_url = 'https://www.imdb.com'
    url = '{}/title/tt6998518/?ref_=fn_al_tt_1'.format(base_url)
    title = url.split('/')[-2]
    crawler = Crawl(url)
    data = crawler.get_data()
    urls = crawler.get_url(base_url)
    with open(title, 'w') as f:
        f.write(data)
    while urls:
        url = urls.pop()
        title = url.split('/')[-2]
        data = crawler.get_data()

if __name__ == '__main__':
    crawler = Crawl()
    crawler.start()
    # save_to_file()
    # col = PyMongo()
    # base_url = 'https://www.imdb.com'
    # url = '{}/title/tt6998518/?ref_=fn_al_tt_1'.format(base_url)
    # crawler = Crawl(url)
    # data = crawler.get_data()
    # col.insert(data)
    # urls = crawler.get_url(base_url)
    # for url in urls:
    #   if not col.check_deplicate(url):
    #       crawler = Crawl(url)
    #       data = crawler.get_data()
    #       col.insert(data)
    #       urls += crawler.get_url(base_url)
    #       logging.info('inserting raw page from {}'.format(url))
    #   else:
    #       logging.info('duplicated link: {}'.format(url))
    #   urls.remove(url)
    #   logging.info('remaining urls: {}'.format(len(urls)))
