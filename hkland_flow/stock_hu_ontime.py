import json
import requests


class SSEStatsOnTime(object):
    """
    http://www.sse.com.cn/services/hkexsc/home/

    """
    def __init__(self):
        self.url = 'http://yunhq.sse.com.cn:32041//v1/hkp/status/amount_status'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
        }

    def get_balance_info(self):
        resp = requests.get(self.url)
        if resp.status_code == 200:
            datas = json.loads(resp.text)
            item = dict()
            # 交易所所属类型
            item['Category'] = "SH"

            # 当前的时间
            m_today = str(datas['date'])
            m_today = "-".join([m_today[:4], m_today[4:6], m_today[6:8]])
            m_time = str(datas['status'][0][1])
            # 区分小时时间是 2 位数和 1 位数的 即 9 点以及之前的数据 10 点以及之后的数据
            if len(m_time) >= 9:    # {'date': 20200417, 'status': [[100547000, 100547000], [417, 418], ['3       ', '111     '], 42000000000, 41207590461, '2']}
                m_time = ":".join([m_time[:2], m_time[2:4], m_time[4:6]])
            else: # {'date': 20200417, 'status': [[94338000, 94337000], [417, 418], ['3       ', '111     '], 42000000000, 41543482907, '2']}
                m_time = ":".join([m_time[:1], m_time[1:3], m_time[3:5]])
            _time = " ".join([m_today, m_time])
            item['Time'] = _time

            # 当日额度
            item['DailyLimit'] = datas['status'][3]
            # 当日资金余额
            item['Balance'] = datas['status'][4]
            # print(item)
            return item


if __name__ == "__main__":
    sse = SSEStatsOnTime()
    sse.get_balance_info()
