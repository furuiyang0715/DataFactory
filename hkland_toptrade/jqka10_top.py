import execjs
import requests
from lxml import html

from hkland_toptrade.configs import JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD, JUY_DB, PRODUCT_MYSQL_HOST, \
    PRODUCT_MYSQL_PORT, PRODUCT_MYSQL_USER, PRODUCT_MYSQL_PASSWORD, PRODUCT_MYSQL_DB
from hkland_toptrade.sql_pool import PyMysqlPoolBase


class JqkaTop10(object):
    # 聚源数据库
    juyuan_cfg = {
        "host": JUY_HOST,
        "port": JUY_PORT,
        "user": JUY_USER,
        "password": JUY_PASSWD,
        "db": JUY_DB,
    }

    # 正式库 在正式环境对标 datacenter
    product_cfg = {
        "host": PRODUCT_MYSQL_HOST,
        "port": PRODUCT_MYSQL_PORT,
        "user": PRODUCT_MYSQL_USER,
        "password": PRODUCT_MYSQL_PASSWORD,
        "db": PRODUCT_MYSQL_DB,
    }

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
        self.juyuan_client = None
        self.product_client = None
        self.table_name = 'hkland_toptrade'

    def _init_pool(self, cfg: dict):
        """
        eg.
        conf = {
                "host": LOCAL_MYSQL_HOST,
                "port": LOCAL_MYSQL_PORT,
                "user": LOCAL_MYSQL_USER,
                "password": LOCAL_MYSQL_PASSWORD,
                "db": LOCAL_MYSQL_DB,
        }
        :param cfg:
        :return:
        """
        pool = PyMysqlPoolBase(**cfg)
        return pool

    def _juyuan_init(self):
        if not self.juyuan_client:
            self.juyuan_client = self._init_pool(self.juyuan_cfg)

    def _product_init(self):
        if not self.product_client:
            self.product_client = self._init_pool(self.product_cfg)

    def __del__(self):
        if self.juyuan_client:
            self.juyuan_client.dispose()
        if self.product_client:
            self.product_client.dispose()

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

    def _create_table(self):
        sql = '''
        CREATE TABLE IF NOT EXISTS `{}` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
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
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `un` (`SecuCode`,`Date`,`CategoryCode`) USING BTREE,
          UNIQUE KEY `un2` (`InnerCode`,`Date`,`CategoryCode`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='陆港通十大成交股';
        '''.format(self.table_name)
        self.product_client.insert(sql)
        self.product_client.end()

    def start(self):
        self._juyuan_init()
        self._product_init()

        self._create_table()

        # 类别代码:GGh: 港股通(沪), GGs: 港股通(深), HG: 沪股通, SG: 深股通'
        category_map = {
            "HG": "http://data.10jqka.com.cn/hgt/hgtb/",
            "GGh": "http://data.10jqka.com.cn/hgt/ggtb/",
            "SG": "http://data.10jqka.com.cn/hgt/sgtb/",
            "GGs": "http://data.10jqka.com.cn/hgt/ggtbs/",
        }
        allitems = []
        for category, url in category_map.items():
            # print(category, url)
            items = self.get_top(category, url)
            allitems.extend(items)

        print(len(allitems))
        for item in allitems:
            print(item)

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
                item['CategoryCode'] = category
                inner_code = self.get_juyuan_codeinfo(item['SecuCode'], category)
                item['InnerCode'] = inner_code
                items.append(item)

        # for item in items:
        #     print(item)

        return items


if __name__ == "__main__":
    jqka = JqkaTop10()
    # jqka.get_top()
    jqka.start()
