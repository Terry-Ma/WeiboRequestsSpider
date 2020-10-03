'''
@Author : Yunquan Ma
@Time   : 2020/10/01
@Email  : rssmyq@aliyun.com
'''

import pymongo
import sys

from utils import ArticleSpider, CommentSpider

def build_pool(pool_name):
    if pool_name not in ('cookies', 'proxies'):
        raise Exception('pool name error !!')
    mongo_client = pymongo.MongoClient(host='localhost', port=27017)
    collection = mongo_client['weibo'][pool_name]
    collection.drop()
    with open('./{}'.format(pool_name)) as f:
        for line in f:
            line = line.replace('\n', '')
            collection.insert_one({pool_name: line, 'status': 'success'})

if __name__ == '__main__':
    build_pool('cookies')
    build_pool('proxies')
    mode2spider = {
        'article': ArticleSpider,
        'comment': CommentSpider
    }
    spider = mode2spider[sys.argv[1]]()
    spider.run()