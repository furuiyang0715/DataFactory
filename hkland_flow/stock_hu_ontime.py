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
            m_time = ":".join([m_time[:2], m_time[2:4], m_time[4:6]])
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
