from urllib.request import urlopen
from bs4 import BeautifulSoup
import re
from pymongo import MongoClient
import logging
logging.basicConfig(level=logging.INFO)

class PyMongo:
	def __init__(self):
		self.col_raw = MongoClient('localhost', 27017).IMDB.raw

	def insert(self, data):
		self.col_raw.insert(data)

	def check_deplicate(self, url):
		return self.col_raw.find_one({'url': url})

class Crawl:
	def __init__(self, url):
		self.url = url
		try:
			self.html = urlopen(url).read()
		except Exception as e:
			print(url, '\n', e)
		self.bsObj = BeautifulSoup(self.html, 'html.parser')


	def get_data(self):		
		title = self.bsObj.title.string
		data = {
				'url': self.url,
				'title': title,
				'raw': self.html,
		}
		return data

	def get_url(self, base_url):
		urls = []
		for link in self.bsObj.findAll('a', href=re.compile('^(/title/)((?!:).).*(/?ref_=tt_rec_tti)+.*$')):
			if 'href' in link.attrs:
				urls.append('{}{}'.format(base_url, link.attrs['href']))
		return urls



if __name__ == '__main__':
	col = PyMongo()
	base_url = 'https://www.imdb.com'
	url = '{}/title/tt6998518/?ref_=fn_al_tt_1'.format(base_url)
	crawler = Crawl(url)
	data = crawler.get_data()
	col.insert(data)
	urls = crawler.get_url(base_url)
	for url in urls:
		if not col.check_deplicate(url):
			crawler = Crawl(url)
			data = crawler.get_data()
			col.insert(data)
			urls += crawler.get_url(base_url)
			logging.info('inserting raw page from {}'.format(url))
		else:
			logging.info('duplicated link: {}'.format(url))
		urls.remove(url)
		logging.info('remaining urls: {}'.format(len(urls)))
