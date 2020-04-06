import json
import requests


class SZSEStatsOnTime(object):
    """http://www.szse.cn/szhk/index.html"""
    def __init__(self):
        self.url = 'http://www.szse.cn/api/market/sgt/dailyamount'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
        }

    def get_balance_info(self):
        resp = requests.get(self.url)
        if resp.status_code == 200:
            datas = json.loads(resp.text)
            # print(datas)
            item = dict()
            # 交易所所属类型
            item['Category'] = "SZ"
            # 当前的时间
            item['Time'] = datas.get("gxsj")
            # 当日资金余额
            item['Balance'] = int(datas['edye']) * pow(10, 6)
            # 每日额度
            item['DailyLimit'] = int(datas['mred']) * pow(10, 8)
            # print(item)
            return item


if __name__ == "__main__":
    sz = SZSEStatsOnTime()
    sz.get_balance_info()
