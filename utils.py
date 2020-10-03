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

logging.getLogger('urllib3').setLevel(logging.WARNING)

class BaseSpider:
    def __init__(self):
        with open('./conf.yml') as f:
            self.config = yaml.load(f, Loader=yaml.FullLoader)
        self.mongo_client = pymongo.MongoClient(host='localhost', port=27017)
        self.pool_collection = {
            'cookies': self.mongo_client['weibo']['cookies'],
            'proxies': self.mongo_client['weibo']['proxies']
            }
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36'
            }
        self.proxies = {
            'https': 'https://220.174.236.211:8091'
            }
        
    def pool_random_select(self, pool_name):
        pool_items = self.pool_collection[pool_name].find({'status': 'success'})
        pool_item_num = pool_items.count()
        if pool_item_num == 0:
            raise Exception('The number of {} is zero; the spider will shutdown!'.format(pool_name))
        pool_item_index = random.randint(0, pool_item_num - 1)
        return pool_items[pool_item_index][pool_name]
    
    def error_status_code(self, url, status_code):
        if status_code in (302, 403):
            pool_name = 'cookies'
        elif status_code == 418:
            pool_name = 'proxies'
        logging.warning('request status {}, url: {}, current {} is unvaild'.format(status_code, url, pool_name))
        self.pool_collection[pool_name].find_one_and_update(
            {pool_name: self.headers['cookies'] if pool_name == 'cooloes' else self.proxies['https']},
            {'$set': {'status': 'error'}})

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
        self.max_page = 30
        self.area = '北京'
        logging.basicConfig(filename='article_spider.log', level=logging.DEBUG,
                            format='%(asctime)s [%(levelname)s] - %(message)s')
    
    def run(self):
        for url in self.get_urls():
            page_list = self.get_page(url)
            for cur_page in page_list:
                cur_url = '{0}&page={1}'.format(url, cur_page)
                try:
                    result = self.request(cur_url)
                    if result:
                        self.collection.insert_many(result)
                        logging.info('parse success, url {}'.format(cur_url))
                    else:
                        logging.info('parse fail, url {}'.format(cur_url))
                except Exception as e:
                    logging.error('request fail, url {}, error {}'.format(cur_url, e), exc_info=True)
                time.sleep(self.config['article']['crawl_delay'])
            logging.info('current url {0}, current total article num {1}, cookie num {2}, proxies num {3}'.\
                format(url, self.collection.count(), 
                       self.pool_collection['cookies'].find({'status': 'success'}).count(),
                       self.pool_collection['proxies'].find({'status': 'success'}).count()))
                
    def request(self, url):
        self.headers['cookie'] = self.pool_random_select('cookies')
        self.proxies['https'] = self.pool_random_select('proxies')
        resp = requests.get(url, headers=self.headers, proxies=self.proxies)
        logging.info('requests status code {}, url {}'.format(resp.status_code, url))
        if resp.status_code != 200:
            self.error_status_code(url, resp.status_code)
            return None
        
        result = []
        soup = BeautifulSoup(resp.text, 'lxml')
        for raw_info in soup.select('div[class = "c"][id]'):
            instance = {}
            instance['weibo_id'] = raw_info['id']
            instance['content'] = raw_info.select('span[class = "ctt"]')[0].get_text()
            instance['create_time'] = raw_info.select('span[class = "ct"]')[0].get_text()[:12]
            instance['crawl_time'] = int(time.time())
            instance['comment_url'] = raw_info.select('a[class = "cc"]')[0]['href']
            instance['area'] = self.area
            result.append(instance)
        
        return result  
        
    def get_urls(self):
        for key, crawl_content in self.config['article']['crawl_contents'].items():
            self.max_page = crawl_content['max_page']
            self.area = crawl_content['area']
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
                    yield cur_url
                    date_begin = next_time

    def get_page(self, url):
        self.headers['cookie'] = self.pool_random_select('cookies')
        self.proxies['https'] = self.pool_random_select('proxies')
        resp = requests.get(url, headers=self.headers, proxies=self.proxies)
        logging.info('requests status code {}, url {}'.format(resp.status_code, url))
        if resp.status_code != 200:
            self.error_status_code(url, resp.status_code)
            return self.get_page(url)
        try:
            soup = BeautifulSoup(resp.text, 'lxml')
            page_str = soup.select('div[class = "pa"] > form > div')[0].get_text()
            total_page = int(page_str[page_str.find('1/') + 2:-1])
            page_list = list(range(1, total_page + 1))
            if total_page > self.max_page:
                random.shuffle(page_list)
                page_list = page_list[:self.max_page]
        except Exception as e:
            logging.warning('get page fail, url {0}, use max_page'.format(url))
            return list(range(1, self.max_page + 1))
        
        return page_list

class CommentSpider(BaseSpider):
    def __init__(self):
        pass
        logging.basicConfig(filename='comment_spider.log', level=logging.DEBUG,
                            format='%(asctime)s [%(levelname)s] - %(message)s')
    
if __name__ == '__main__':
    base_spider = BaseSpider()
    print(base_spider.config)