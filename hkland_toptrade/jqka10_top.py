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

from hkland_toptrade.base_spider import BaseSpider


class JqkaTop10(BaseSpider):
    """十大成交股 同花顺数据源"""
    def __init__(self):
        super(JqkaTop10, self).__init__()
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

    def get_juyuan_codeinfo(self, secu_code, market):
        """获取聚源内部编码"""
        sql1 = 'SELECT SecuCode,InnerCode from SecuMain WHERE SecuCategory in (1, 2, 8) \
and SecuMarket in (83, 90) \
and ListedSector in (1, 2, 6, 7) and SecuCode = "{}";'.format(secu_code)

        sql2 = 'SELECT SecuCode,InnerCode from hk_secumain WHERE SecuCategory in (51, 3, 53, 78) \
        and SecuMarket in (72) and ListedSector in (1, 2, 6, 7) and SecuCode = "{}";'.format(secu_code)

        if market in ("HG", "SG"):
            ret = self.juyuan_client.select_one(sql1)
        elif market in ("GGh", "GGs"):
            ret = self.juyuan_client.select_one(sql2)
        else:
            raise
        return ret.get('InnerCode')

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
        self._juyuan_init()
        self._product_init()
        # 建表 直接入pro库 建表报错; 在首次建表之后此步骤忽略
        # self._create_table()

        # 类别代码:GGh: 港股通(沪), GGs: 港股通(深), HG: 沪股通, SG: 深股通'
        # value 中的网址也是 web 网址
        category_map = {
            "HG": "http://data.10jqka.com.cn/hgt/hgtb/",
            "GGh": "http://data.10jqka.com.cn/hgt/ggtb/",
            "SG": "http://data.10jqka.com.cn/hgt/sgtb/",
            "GGs": "http://data.10jqka.com.cn/hgt/ggtbs/",
        }
        allitems = []
        for category, url in category_map.items():
            items = self.get_top(category, url)
            allitems.extend(items)

        self._batch_save(self.product_client, allitems, self.table_name, self.fields)
        self.refresh_update_time()

    def get_top(self, category, url):
        body = self.get(url)
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
        # 判断十大成交股的时间
        page_dts = doc.xpath(".//div[@class='table-tit']")
        dt_infos = [page_dt.text_content().replace("\r\n", "") for page_dt in page_dts]
        top_str = ''
        for dt_info in dt_infos:
            if "十大成交股" in dt_info:
                top_str = dt_info
        print(top_str)
        top_dt_str = re.findall("\d{4}-\d{2}-\d{2}", top_str)[0]
        # 从网页中解析出来的 十大成交股 的最近时间
        top_dt = datetime.datetime.strptime(top_dt_str, "%Y-%m-%d")
        print("{} 的最近更新时间是 {}".format(category, top_dt))
        top10_table = doc.xpath(".//table[@class='m-table1']")[0]

        # 爬取到网站中的全部字段
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
                item['CategoryCode'] = category
                inner_code = self.get_juyuan_codeinfo(item['SecuCode'], category)
                item['InnerCode'] = inner_code
                # item["Date"] = datetime.datetime.combine(datetime.datetime.today(), datetime.time.min)
                item["Date"] = top_dt
                item['CMFID'] = 1  # 兼容之前的程序 写死
                item['CMFTime'] = datetime.datetime.now()  # 兼容和之前的程序 用当前的时间代替
                items.append(item)
        return items


if __name__ == "__main__":
    jqka = JqkaTop10()
    jqka.start()
