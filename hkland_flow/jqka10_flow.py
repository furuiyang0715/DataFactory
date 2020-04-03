import datetime
import re

import execjs
import requests


class SFLgthisdataspiderSpider(object):
    def __init__(self):
        self.headers = {
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            # 'hexin-v': '',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
            'Accept': 'text/html, */*; q=0.01',
            'Referer': 'http://data.10jqka.com.cn/hgt/ggtb/',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
        }
        self.base_url = 'http://data.10jqka.com.cn/hgt/{}/'
        self.category_map = {
            'hgtb': '沪股通',
            'ggtb': '港股通(沪)',
            'sgtb': '深股通',
            'ggtbs': '港股通(深)',
        }
        self.today = datetime.datetime.today().strftime("%Y-%m-%d")

    @property
    def cookies(self):
        with open('jqka.js', 'r') as f:
            jscont = f.read()
        cont = execjs.compile(jscont)
        cookie_v = cont.call('v')
        cookies = {
            'v': cookie_v,
        }
        return cookies

    def get(self, url):
        resp = requests.get(url, headers=self.headers, cookies=self.cookies)
        if resp.status_code == 200:
            return resp.text

    def _check_if_trading_today(self):
        pass

    def _start(self):
        for category in self.category_map:
            url = self.base_url.format(category)
            page = self.get(url)
            ret = re.findall(r"var dataDay = (.*);", page)
            if ret:
                datas = eval(ret[0])[0]
                for data in datas:
                    item = dict()
                    item['Date'] = self.today + " " + data[0]
                    item['Flow'] = float(data[1])
                    item['Balance'] = float(data[2])
                    item['Category'] = self.category_map.get(category)
                    item['CategoryCode'] = category
                    print(item)


if __name__ == "__main__":
    sf = SFLgthisdataspiderSpider()
    # print(sf.cookies)
    sf._start()


    pass