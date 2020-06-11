import datetime
import os
import re
import sys
import execjs
import requests
from lxml import html


cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)

from hkland_historytradestat.base_spider import BaseSpider


class JqkaHistory(BaseSpider):
    def __init__(self):
        super(JqkaHistory, self).__init__()
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
        self.market_map = {
            "HG": (1, "沪股通"),
            "SG": (3, "深股通"),
            "GGh": (2, "港股通(沪市)"),
            "GGs": (4, "港股通(深市)"),

            # 1 | 沪股通
            # 2 | 港股通(沪市)
            # 3 | 深股通
            # 4 | 港股通(深市)

        }
        self.hk_sh_his = self.select_last_total(1).get("MoneyInHistoryTotal")
        self.hk_sz_his = self.select_last_total(3).get("MoneyInHistoryTotal")
        self.sh_hk_his = self.select_last_total(2).get("MoneyInHistoryTotal")
        self.sz_hk_his = self.select_last_total(4).get("MoneyInHistoryTotal")
        self.fields = ["Date", 'MoneyInHistoryTotal', 'MarketTypeCode', 'MarketType',
                       'MoneyIn', "MoneyBalance", "NetBuyAmount", "BuyAmount", "SellAmount"]

    def select_last_total(self, market_type):
        """查找距给出时间最近的一个时间点的累计值"""
        sql = '''select Date, MoneyInHistoryTotal from hkland_historytradestat where Date = (select max(Date) \
        from hkland_historytradestat where MarketTypeCode = {}) and MarketTypeCode = {};'''.format(market_type, market_type)
        ret = self.dc_client.select_one(sql)
        return ret

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

    @staticmethod
    def re_decimal_data(data):
        if isinstance(data, str):
            data = float(data)
        ret = float("%.4f" % data)
        return ret

    def re_str_data(self, data_str: str):
        if data_str.endswith("万"):
            data = self.re_decimal_data(data_str.replace("万", '')) / 10**2
        elif data_str.endswith("亿"):
            data = self.re_decimal_data(data_str.replace("亿", '')) * 10**2
        else:
            raise
        return data

    def start(self):
        self._juyuan_init()
        self._product_init()

        # self._create_table()

        # 类别代码:GGh: 港股通(沪), GGs: 港股通(深), HG: 沪股通, SG: 深股通'
        category_map = {
            "HG": "http://data.10jqka.com.cn/hgt/hgtb/",
            "GGh": "http://data.10jqka.com.cn/hgt/ggtb/",
            "SG": "http://data.10jqka.com.cn/hgt/sgtb/",
            "GGs": "http://data.10jqka.com.cn/hgt/ggtbs/",    # 无历史数据
        }
        allitems = []
        for category, url in category_map.items():
            items = self.get_history(category, url)
            allitems.extend(items)

        # self._batch_save(self.product_client, allitems, self.table_name, self.fields)
        # self.refresh_update_time()

    def get_history(self, category, url):
        body = self.get(url)
        doc = html.fromstring(body)
        page_dts = doc.xpath(".//div[@class='table-tit']")
        dt_infos = [page_dt.text_content().replace("\r\n", "") for page_dt in page_dts]
        top_str = ''
        for dt_info in dt_infos:
            if "历史数据" in dt_info:
                top_str = dt_info

        if not top_str:
            return []

        print(top_str)
        top_dt_str = re.findall("\d{4}-\d{2}-\d{2}", top_str)[0]
        top_dt = datetime.datetime.strptime(top_dt_str, "%Y-%m-%d")
        print("{} 的最近更新时间是 {}".format(category, top_dt))
        '''
        <table class="m-table J-ajax-table">
            <thead>
                <tr>
                    <th width="100">日期</th>
                    <th width="100" colidx="1">当日资金流入<br>(元)</th>
                    <th width="100">当日余额<br>(元)</th>
                    <th width="120">当日成交净买额<br>(元)</th>
                    <th width="100">买入成交额<br>(元)</th>
                    <th width="100">卖出成交额<br>(元)</th>
                    <th width="110">领涨股</th>
                    <th width="110">领涨股涨跌幅</th>
                    <th width="100">上证指数</th>
                    <th>涨跌幅</th>
                </tr>
            </thead>
            <tbody>
            <tr class="">
                    <td>2020-06-10</td>
                    <td class="c-fall">-5.79亿</td>
                    <td class="tc">525.79亿</td>
                    <td class="c-fall">-12.33亿</td>
                    <td class="">125.46亿</td>
                    <td class="">137.79亿</td>
                    <td><a href="http://stockpage.10jqka.com.cn/600859/" target="_blank">王府井</a></td>
                    <td class="c-rise">10.01%</td>
                    <td class="c-fall">2943.75</td>
                    <td class="c-fall">-0.42%</td>
            </tr>
            <tr class="odd">
                    <td>2020-06-09</td>
                    <td class="c-rise">32.17亿</td>
                    <td class="tc">487.83亿</td>
                    <td class="c-rise">24.93亿</td>
                    <td class="">149.53亿</td>
                    <td class="">124.60亿</td>
                    <td><a href="http://stockpage.10jqka.com.cn/600859/" target="_blank">王府井</a></td>
                    <td class="c-rise">10.00%</td>
                    <td class="c-rise">2956.11</td>
                    <td class="c-rise">0.62%</td>
            </tr>
            <tr class="">
                    <td>2020-06-08</td>
                    <td class="c-rise">21.11亿</td>
                    <td class="tc">498.89亿</td>
                    <td class="c-rise">15.34亿</td>
                    <td class="">163.50亿</td>
                    <td class="">148.15亿</td>
                    <td><a href="http://stockpage.10jqka.com.cn/600230/" target="_blank">沧州大化</a></td>
                    <td class="c-rise">10.02%</td>
                    <td class="c-rise">2937.77</td>
                    <td class="c-rise">0.24%</td>
            </tr>
            <tr class="odd">
                    <td>2020-06-05</td>
                    <td class="c-rise">37.09亿</td>
                    <td class="tc">482.91亿</td>
                    <td class="c-rise">29.84亿</td>
                    <td class="">141.37亿</td>
                    <td class="">111.53亿</td>
                    <td><a href="http://stockpage.10jqka.com.cn/600895/" target="_blank">张江高科</a></td>
                    <td class="c-rise">10.02%</td>
                    <td class="c-rise">2930.8</td>
                    <td class="c-rise">0.40%</td>
            </tr>
            <tr class="">
                    <td>2020-06-04</td>
                    <td class="c-rise">10.84亿</td>
                    <td class="tc">509.16亿</td>
                    <td class="c-rise">4.86亿</td>
                    <td class="">134.75亿</td>
                    <td class="">129.88亿</td>
                    <td><a href="http://stockpage.10jqka.com.cn/600565/" target="_blank">迪马股份</a></td>
                    <td class="c-rise">10.13%</td>
                    <td class="c-fall">2919.25</td>
                    <td class="c-fall">-0.14%</td>
            </tr>
            <tr class="odd">
                    <td>2020-06-03</td>
                    <td class="c-rise">12.71亿</td>
                    <td class="tc">507.29亿</td>
                    <td class="c-rise">6.62亿</td>
                    <td class="">162.59亿</td>
                    <td class="">155.96亿</td>
                    <td><a href="http://stockpage.10jqka.com.cn/600565/" target="_blank">迪马股份</a></td>
                    <td class="c-rise">10.11%</td>
                    <td class="c-rise">2923.37</td>
                    <td class="c-rise">0.07%</td>
            </tr>
            <tr class="">
                    <td>2020-06-02</td>
                    <td class="c-rise">43.93亿</td>
                    <td class="tc">476.07亿</td>
                    <td class="c-rise">37.35亿</td>
                    <td class="">179.92亿</td>
                    <td class="">142.57亿</td>
                    <td><a href="http://stockpage.10jqka.com.cn/600828/" target="_blank">茂业商业</a></td>
                    <td class="c-rise">10.02%</td>
                    <td class="c-rise">2921.4</td>
                    <td class="c-rise">0.20%</td>
            </tr>
            <tr class="odd">
                    <td>2020-06-01</td>
                    <td class="c-rise">46.42亿</td>
                    <td class="tc">473.58亿</td>
                    <td class="c-rise">39.90亿</td>
                    <td class="">188.95亿</td>
                    <td class="">149.05亿</td>
                    <td><a href="http://stockpage.10jqka.com.cn/600337/" target="_blank">美克家居</a></td>
                    <td class="c-rise">10.11%</td>
                    <td class="c-rise">2915.43</td>
                    <td class="c-rise">2.21%</td>
            </tr>
            <tr class="">
                    <td>2020-05-29</td>
                    <td class="c-rise">20.80亿</td>
                    <td class="tc">499.20亿</td>
                    <td class="c-rise">10.82亿</td>
                    <td class="">185.03亿</td>
                    <td class="">174.22亿</td>
                    <td><a href="http://stockpage.10jqka.com.cn/600185/" target="_blank">格力地产</a></td>
                    <td class="c-rise">10.05%</td>
                    <td class="c-rise">2852.35</td>
                    <td class="c-rise">0.21%</td>
            </tr>
            <tr class="odd">
                    <td>2020-05-28</td>
                    <td class="c-rise">32.09亿</td>
                    <td class="tc">487.91亿</td>
                    <td class="c-rise">25.70亿</td>
                    <td class="">158.43亿</td>
                    <td class="">132.73亿</td>
                    <td><a href="http://stockpage.10jqka.com.cn/600185/" target="_blank">格力地产</a></td>
                    <td class="c-rise">10.07%</td>
                    <td class="c-rise">2846.22</td>
                    <td class="c-rise">0.33%</td>
            </tr>
            </tbody>
            </table>
        '''

        history_table = doc.xpath(".//table[@class='m-table J-ajax-table']")[0]

        # table_heads = history_table.xpath("./thead/tr/th")
        # if table_heads:
        #     table_heads = [table_head.text for table_head in table_heads]
        #     print(table_heads)    # ['日期', '当日资金流入', '当日余额', '当日成交净买额', '买入成交额', '卖出成交额', '领涨股', '领涨股涨跌幅', '上证指数', '涨跌幅']
        '''
        `Date` datetime NOT NULL COMMENT '日期',
        `MoneyIn` decimal(20,4) NOT NULL COMMENT '当日资金流入(百万）',
        `MoneyBalance` decimal(20,4) NOT NULL COMMENT '当日余额（百万）',
        `MoneyInHistoryTotal` decimal(20,4) NOT NULL COMMENT '历史资金累计流入(百万元）',
        `NetBuyAmount` decimal(20,4) NOT NULL COMMENT '当日成交净买额(百万元）',
        `BuyAmount` decimal(20,4) NOT NULL COMMENT '买入成交额(百万元）',
        `SellAmount` decimal(20,4) NOT NULL COMMENT '卖出成交额(百万元）',
        `MarketTypeCode` int(11) NOT NULL COMMENT '市场类型代码',
        `MarketType` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '市场类型',
        `CMFID` bigint(20) NOT NULL COMMENT '来源ID',
        `CMFTime` datetime NOT NULL COMMENT '来源日期',
        '''

        table_heads = ["Date",           # 日期
                       "MoneyIn",        # 当日资金流入(百万）
                       "MoneyBalance",   # 当日余额
                       "NetBuyAmount",   # 当日成交净买额(百万元）
                       "BuyAmount",      # 买入成交额(百万元）
                       "SellAmount",     # 卖出成交额(百万元）
                       "headstock",      # 领涨股
                       "headstockrise",  # 领涨股涨跌幅
                       "index",          # 上证或者恒生指数
                       "rate",           # 涨跌幅
                       ]
        unfields = ["headstock", "headstockrise", "index", "rate"]
        moneyfields = ['MoneyIn', "MoneyBalance", "NetBuyAmount", "BuyAmount", "SellAmount"]
        items = []
        allhistory = history_table.xpath(".//tbody/tr")[:1]    # 只要最近一天的数据
        # print(allhistory)
        for history in allhistory:
            trs = history.xpath("./td")
            if trs:
                top_info = [tr.text_content() for tr in trs]
                item = dict(zip(table_heads, top_info))
                for field in unfields:
                    item.pop(field)
                for field in moneyfields:
                    item[field] = self.re_str_data(item.get(field))
                item["Date"] = top_dt
                # 历史资金累计流入(百万) = 上一天的历史资金累计流入(百万) + 今天的当日成交净买额(百万元)
                item['MoneyInHistoryTotal'] = ''
                _type_code, _type = self.market_map.get(category)
                item['MarketTypeCode'] = _type_code
                item['MarketType'] = _type
                item['CMFID'] = 1  # 兼容之前的程序 写死
                item['CMFTime'] = datetime.datetime.now()  # 兼容和之前的程序 用当前的时间代替
                items.append(item)
                print(item)
        return items


if __name__ == "__main__":
    jqka = JqkaHistory()
    jqka.start()
