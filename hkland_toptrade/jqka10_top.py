import execjs
import requests
from lxml import html


class JqkaTop10(object):
    def __init__(self):
        self.url = 'http://data.10jqka.com.cn/hgt/hgtb/'
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
            data = self.re_decimal_data(data_str.replace("万", '')) * 10**4
        elif data_str.endswith("亿"):
            data = self.re_decimal_data(data_str.replace("亿", '')) * 10**8
        else:
            raise
        return data

    def start(self):
        body = self.get(self.url)
        # 十大成交股的时间
        '''
        <div class="table-tit">
            <h2 class="icon-table">沪股通十大成交股<span class="hgt-text"></span></h2>
            <div class="curtime">
                <span class="fr">2020-06-08（周一）</span>
            </div>
        </div>
        '''
        doc = html.fromstring(body)
        # top10_dt = doc.xpath(".//div[@class='table-tit']")
        top10_table = doc.xpath(".//table[@class='m-table1']")[0]

        # table_heads = top10_table.xpath("./thead/tr/th")
        # if table_heads:
        #     table_heads = [table_head.text for table_head in table_heads]
        #     print(table_heads)    # ['排名', '股票代码', '股票简称', '收盘价', '涨跌幅', '涨跌额', '买入金额', '卖出金额', '净买额', '成交金额']

        '''
        `Date` date NOT NULL COMMENT '时间',
        `SecuCode` varchar(10) COLLATE utf8_bin NOT NULL COMMENT '证券代码',
        `InnerCode` int(11) NOT NULL COMMENT '内部编码',
        `SecuAbbr` varchar(20) COLLATE utf8_bin NOT NULL COMMENT '股票简称',
        `Close` decimal(19,3) NOT NULL COMMENT '收盘价',
        `ChangePercent` decimal(19,5) NOT NULL COMMENT '涨跌幅',
        `TJME` decimal(19,3) NOT NULL COMMENT '净买额（元/港元）',
        `TMRJE` decimal(19,3) NOT NULL COMMENT '买入金额（元/港元）',
        `TCJJE` decimal(19,3) NOT NULL COMMENT '成交金额（元/港元）',
        `CategoryCode` varchar(10) COLLATE utf8_bin DEFAULT NULL COMMENT '类别代码:GGh: 港股通(沪), GGs: 港股通(深), HG: 沪股通, SG: 深股通',
        `CMFID` bigint(20) NOT NULL COMMENT '来源ID',
        `CMFTime` datetime NOT NULL COMMENT '来源日期',
        '''

        table_heads = ["rank",       # 排名
                       "SecuCode",   # 股票代码
                       "SecuAbbr",   # 股票简称
                       "Close",      # 收盘价
                       "ChangePercent",   # 涨跌幅
                       "changeamount",    # 涨跌额
                       "TMRJE",        # 买入金额
                       "sellamount",   # 卖出金额
                       "TJME",   # 净买额
                       "TCJJE",  # 成交金额
                       ]
        unfields = ["rank", "changeamount", "sellamount"]
        moneyfields = ['TMRJE', "TJME", "TCJJE"]
        items = []
        top10 = top10_table.xpath(".//tbody/tr")
        for top in top10:
            trs = top.xpath("./td")
            if trs:
                top_info = [tr.text_content() for tr in trs]
                item = dict(zip(table_heads, top_info))
                for field in unfields:
                    item.pop(field)
                for field in moneyfields:
                    item[field] = self.re_str_data(item.get(field))
                item['ChangePercent'] = item['ChangePercent'].replace("%", "")
                items.append(item)

        for item in items:
            print(item)


if __name__ == "__main__":
    jqka = JqkaTop10()
    jqka.start()
