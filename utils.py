'''
@Author : Yunquan Ma
@Time   : 2020/10/01
@Email  : rssmyq@aliyun.com
'''

import requests
import pymongo
import yaml
import logging
import random
import time
import datetime

from bs4 import BeautifulSoup
from retrying import retry

logging.getLogger('urllib3').setLevel(logging.WARNING)

class BaseSpider:
    def __init__(self):
        with open('./conf.yml') as f:
            self.config = yaml.load(f, Loader=yaml.FullLoader)
        self.mongo_client = pymongo.MongoClient(host='localhost', port=27017)
        self.cookie_collection = self.mongo_client['weibo']['cookies']
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36'
        }
        
    def get_cookie(self):
        cookies = self.cookie_collection.find({'status': 'success'})
        cookies_num = cookies.count()
        if cookies_num == 0:
            raise Exception('The number of cookies is zero; the spider will shutdown!')
        cookies_index = random.randint(0, cookies_num - 1)
        return cookies[cookies_index]['cookie']

class ArticleSpider(BaseSpider):
    def __init__(self):
        super().__init__()
        self.collection = self.mongo_client[self.config['article']['output']['database']]\
            [self.config['article']['output']['collection']]
        self.url_format = 'https://weibo.cn/search/mblog?hideSearchFrame=&keyword={}' +\
            '&advancedfilter=1&starttime={}&endtime={}'
        self.general_keywords = []
        with open('./{}'.format(self.config['article']['keywords_path'])) as f:
            for line in f:
                self.general_keywords.append(line.replace('\n', ''))
        logging.basicConfig(filename='article_spider.log', level=logging.DEBUG,
                            format='%(asctime)s [%(levelname)s] - %(message)s')
    
    def run(self):
        for url, max_page in self.get_urls():
            page = self.get_page(url, max_page)
            for cur_page in range(1, page + 1):
                cur_url = '{0}&page={1}'.format(url, cur_page)
                try:
                    result = self.request(cur_url)
                    if result:
                        self.collection.insert_one(result)
                except Exception as e:
                    logging.error('request fail, url {}, error {}'.format(cur_url, e), exc_info=True)
                time.sleep(self.config['article']['crawl_delay'])
            logging.info('present article num {0}, present cookie num {1}'.\
                format(self.collection.count(), self.cookie_collection.find({'status': 'success'}).count()))
                
    def request(self, url):
        self.headers['cookie'] = self.get_cookie()
        resp = requests.get(url, headers=self.headers)
        if resp.status_code != 200:
            if resp.status_code in (302, 403):
                self.del_cookie(url)
            return None
        result = {}
        soup = BeautifulSoup(resp.text, 'lxml')
        for raw_info in soup.select('div[class = "c"][id]'):
            result['weibo_id'] = raw_info['id']
            result['content'] = raw_info.select('span[class = "ctt"]')[0].get_text()
            result['create_time'] = raw_info.select('span[class = "ct"]')[0].get_text()[:12]
            result['crawl_time'] = int(time.time())
            result['comment_url'] = raw_info.select('a[class = "cc"]')[0]['href']
        
        return result  
        
    def get_urls(self):
        for key, crawl_content in self.config['article']['crawl_contents'].items():
            date_begin = datetime.datetime.strptime(crawl_content['begin_date'], '%Y-%m-%d')
            date_end = datetime.datetime.strptime(crawl_content['end_date'], '%Y-%m-%d')
            time_spread = datetime.timedelta(days=1)
            while date_begin < date_end:
                next_time = date_begin + time_spread
                for cur_general_keyword in self.general_keywords:
                    cur_url = self.url_format.format(
                        crawl_content['area'] + cur_general_keyword,
                        date_begin.strftime("%Y%m%d"),
                        next_time.strftime("%Y%m%d")
                        )
                    yield cur_url, crawl_content['max_page']
                    date_begin = next_time

    def get_page(self, url, max_page):
        self.headers['cookie'] = self.get_cookie()
        resp = requests.get(url, headers=self.headers)
        if resp.status_code != 200:
            if resp.status_code in (302, 403):
                self.del_cookie(url)
            return self.get_page(url, max_page)
        try:
            soup = BeautifulSoup(resp.text, 'lxml')
            page_str = soup.select('div[class = "pa"] > form > div')[0].get_text()
            total_page = int(page_str[page_str.find('1/') + 2:-1])
            page = min(max_page, total_page)
        except Exception as e:
            logging.warning('get page fail, url {0}, set page {1}'.format(url, max_page))
            return max_page
        
        return page
    
    def del_cookie(self, url):
        logging.warning('request status != 200, url: {}, current cookie is unvaild'.format(url))
        self.cookie_collection.find_one_and_update({'cookie': self.headers['cookie']},
                                                   {'$set': {'status': 'error'}})

class CommentSpider(BaseSpider):
    def __init__(self):
        pass
        logging.basicConfig(filename='comment_spider.log', level=logging.DEBUG,
                            format='%(asctime)s [%(levelname)s] - %(message)s')
    
if __name__ == '__main__':
    base_spider = BaseSpider()
    print(base_spider.config)