'''
@Author : Yunquan Ma
@Time   : 2020/10/01
@Email  : rssmyq@aliyun.com
'''

import pymongo
import sys

from utils import ArticleSpider, CommentSpider

def insert_cookies():
    mongo_client = pymongo.MongoClient(host='localhost', port=27017)
    collection = mongo_client['weibo']['cookies']
    collection.drop()
    with open('./cookies') as f:
        for line in f:
            cookie_str = line.replace('\n', '')
            collection.insert_one({'cookie': cookie_str, 'status': 'success'})

if __name__ == '__main__':
    insert_cookies()
    mode2spider = {
        'article': ArticleSpider,
        'comment': CommentSpider
    }
    spider = mode2spider[sys.argv[1]]()
    spider.run()