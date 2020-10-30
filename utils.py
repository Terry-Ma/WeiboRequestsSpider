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
import re
import numpy as np

from bs4 import BeautifulSoup

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
            logging.error('The number of cookies is zero; the spider will shutdown!')
            raise Exception('The number of cookies is zero; the spider will shutdown!')
        cookies_index = random.randint(0, cookies_num - 1)
        return cookies[cookies_index]['cookie']
    
    def del_cookie(self):
        logging.warning('delete current cookie: {}'.format(self.headers['cookie']))
        self.cookie_collection.find_one_and_update({'cookie': self.headers['cookie']},
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
        self.pattern = re.compile(r'')
        logging.basicConfig(filename='article_spider.log', level=logging.DEBUG,
                            format='%(asctime)s [%(levelname)s] - %(message)s')
    
    def run(self):
        for url in self.get_urls():
            for cur_page in range(1, self.max_page + 1):
                cur_url = '{0}&page={1}'.format(url, cur_page)
                resp_results, no_article = self.request(cur_url)
                if no_article:  # no more articles
                    break
                if resp_results:
                    self.collection.insert_many(resp_results)
                time.sleep(np.random.normal(self.config['article']['crawl_delay_mu'],
                                            self.config['article']['crawl_delay_sigma']))
            logging.info('current total article num {0}, cookie num {1}'.\
                format(self.collection.count(), self.cookie_collection.find({'status': 'success'}).count()))
                
    def request(self, url):
        self.headers['cookie'] = self.get_cookie()
        resp = requests.get(url, headers=self.headers)
        logging.info('requests status code {}, url {}'.format(resp.status_code, url))
        if resp.status_code == 418:  # ip banned
            logging.error('ip is banned !! spider will shutdown !!')
            raise Exception('ip is unvalid !!')
        result = []
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, 'lxml')
        try:
            for raw_info in soup.select('div[class = "c"][id]'):
                instance = {}
                instance['weibo_id'] = raw_info['id']
                if len(raw_info.select('span[class = "cmt"]')) == 0:  # not forward
                    instance['is_forward'] = 0
                    instance['content'] = raw_info.select('span[class = "ctt"]')[0].get_text()[1:]
                    instance['comment_url'] = raw_info.select('a[class = "cc"]')[0]['href']
                    instance['origin_content'] = ''
                else:  # forard
                    instance['is_forward'] = 1
                    div_list = raw_info.select('div')
                    index = len(div_list) - 1
                    while index >= 0 and len(div_list[index].select('span[class = "cmt"]')) == 0:
                        index += 1
                    raw_content = div_list[index].get_text()
                    left = raw_content.find('转发理由:')
                    right = raw_content.find('赞[')
                    instance['content'] = raw_content[left + 5:right]
                    instance['comment_url'] = raw_info.select('a[class = "cc"]')[-1]['href']
                    instance['origin_content'] = raw_info.select('span[class = "ctt"]')[0].get_text()
                instance['create_time'] = raw_info.select('span[class = "ct"]')[0].get_text()[:12]
                instance['crawl_time'] = int(time.time())
                instance['area'] = self.area
                instance['comment_crawled'] = 0
                result.append(instance)
        except Exception as e:   # parse error
            logging.error('parse error, url {}'.format(url), exc_info=True)
            return result, False
        if not result:
            tag_list = soup.select('span[class = "pmf"]')
            if (len(tag_list) > 0 and tag_list[0].get_text() == '返回页面顶部') or ('抱歉，未找到' in resp.text):  # no article
                logging.info('no more articles, url {}'.format(url))
                return result, True
            else: 
                logging.warning('parse fail, url {}, delete current cookie and retry!'.format(url))
                self.del_cookie()
                return self.request(url)  # retry
        else:
            logging.info('parse success, url {}, article num {}'.format(url, len(result)))
        
        return result, False
        
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

class CommentSpider(BaseSpider):
    def __init__(self):
        super().__init__()
        logging.basicConfig(filename='comment_spider.log', level=logging.DEBUG,
                            format='%(asctime)s [%(levelname)s] - %(message)s')
        self.article_collection = self.mongo_client[self.config['comment']['origin_article']['database']]\
            [self.config['comment']['origin_article']['collection']]
        self.comment_collection = self.mongo_client[self.config['comment']['output']['database']]\
            [self.config['comment']['output']['collection']]
        self.crawl_page = self.config['comment']['crawl_page']
        self.pattern = re.compile(r'回复.*[:\s]')
        logging.basicConfig(filename='comment_spider.log', level=logging.DEBUG,
                            format='%(asctime)s [%(levelname)s] - %(message)s')
    
    def run(self):
        article_infos = [article_info for article_info in self.article_collection.find() \
            if article_info['comment_crawled'] == 0]
        for article_info in article_infos:
            url_format = article_info['comment_url'].replace('#cmtfrm', '') + '&page={}'
            for cur_page in range(1, self.crawl_page + 1):
                cur_url = url_format.format(cur_page)
                resp_results, no_comment = self.request(cur_url)
                if no_comment:  # no more comments under the current article
                    break
                for resp_result in resp_results:
                    result = {}
                    result['area'] = article_info['area']
                    result['weibo_id'] = article_info['weibo_id']
                    result.update(resp_result)
                    self.comment_collection.insert_one(result)
                time.sleep(np.random.normal(self.config['comment']['crawl_delay_mu'], 
                                            self.config['comment']['crawl_delay_sigma']))
            self.article_collection.update_many({'weibo_id': article_info['weibo_id']},
                                                {'$set': {'comment_crawled': 1}})
            logging.info('current total comment num {}, cookie num {}'.\
                format(self.comment_collection.count(), self.cookie_collection.find({'status': 'success'}).count()))
                
    def request(self, url):
        self.headers['cookie'] = self.get_cookie()
        resp = requests.get(url, headers=self.headers)
        logging.info('requests status code {}, url {}'.format(resp.status_code, url))
        if resp.status_code == 418:  # ip banned
            logging.error('ip is banned !! spider will shutdown !!')
            raise Exception('ip is unvalid !!')
        result = []
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, 'lxml')
        tag_list = soup.select('div[class = "c"]')
        if tag_list and tag_list[-1].get_text() == '还没有人针对这条微博发表评论!':  # no comment
            logging.info('no more comments under the current article, url {}'.format(url))
            return result, True
        try:
            for raw_info in soup.select('div[class = "c"][id]'):
                instance = {}
                instance['comment_id'] = raw_info['id']
                if not instance['comment_id'].startswith('M_'):
                    raw_content = raw_info.select('span[class = "ctt"]')[0].get_text()
                    if raw_content[:2] == '回复':
                        str_split = self.pattern.split(raw_content)
                        if len(str_split) > 1:
                            instance['content'] = str_split[1]
                        else:
                            instance['content'] = raw_content
                        instance['is_reply'] = 1
                    else:
                        instance['content'] = raw_content
                        instance['is_reply'] = 0
                    instance['create_time'] = raw_info.select('span[class = "ct"]')[0].get_text()[:12]
                    result.append(instance)
        except Exception as e:   # parse error
            logging.error('parse error, url {}'.format(url), exc_info=True)
            return result, False
        if not result:
            logging.warning('parse fail, url {}, delete current cookie and retry!'.format(url))
            # self.del_cookie()   # it's difficult to distinguish whether it is caused by cookie
            return self.request(url)  # retry
        else:
            logging.info('parse success, url {}, comment num {}'.format(url, len(result)))
        
        return result, False