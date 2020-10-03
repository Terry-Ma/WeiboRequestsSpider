import requests

from bs4 import BeautifulSoup

proxies = {
    # 'https': 'https://220.174.236.211:8091' 较快
    # 'https': 'https://112.95.188.29:9000' 较慢
    # 'https': 'https://218.60.8.99:3129' 快
    # 'https': 'https://91.238.203.68:3128' 较慢
    # 'https': 'https://14.115.107.1:808' 较慢
    # 'https': 'https://163.204.94.86:9999' 快
}
a = requests.get('https://httpbin.org/get', proxies=proxies)
print(a.text)