import datetime
import logging
import re
import requests
import opencc
from lxml import html

import utils
from hkland_configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, SPIDER_MYSQL_USER,
                            SPIDER_MYSQL_PASSWORD, SPIDER_MYSQL_DB, JUY_HOST, JUY_PORT, JUY_USER,
                            JUY_PASSWD, JUY_DB)
from sql_base import Connection

logger = logging.getLogger()


class SharesSpider(object):
    """滬股通及深股通持股紀錄按日查詢"""
    spider_conn = Connection(
        host=SPIDER_MYSQL_HOST,
        port=SPIDER_MYSQL_PORT,
        user=SPIDER_MYSQL_USER,
        password=SPIDER_MYSQL_PASSWORD,
        database=SPIDER_MYSQL_DB,
    )

    juyuan_conn = Connection(
        host=JUY_HOST,
        port=JUY_PORT,
        user=JUY_USER,
        password=JUY_PASSWD,
        database=JUY_DB,
    )

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
    }

    converter = opencc.OpenCC('t2s')  # 中文繁体转简体

    def __init__(self, mk_type, offset=1):
        """
        默认只更新之前一天的记录
        """
        self.type = mk_type
        self.today = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        self.offset = offset
        self.check_day = (datetime.date.today() - datetime.timedelta(days=self.offset)).strftime("%Y/%m/%d")
        self.url = 'https://www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t={}'.format(type)
        self.market = {"sh": 83, "sz": 90, 'hk': 72}.get(self.type)
        self.type_name = {'sh': '沪股通', 'sz': '深股通', 'hk': '港股通'}.get(self.type)
        self.percent_comment = {
            'sh': '占于上交所上市及交易的A股总数的百分比(%)',
            'sz': '占于深交所上市及交易的A股总数的百分比(%)',
            'hk': '占已发行股份的百分比(%)',
        }.get(self.type)

        # 敏仪的爬虫表名是 hold_shares_sh hold_shares_sz hold_shares_hk
        # 我这边更新后的表名是 hoding_ .. 区别是跟正式表的字段保持一致
        self.spider_table = 'holding_shares_{}'.format(self.type)

        # 生成正式库中的两个 hkland_shares hkland_hkshares
        if self.type in ("sh", "sz"):
            self.table_name = 'hkland_shares'
        elif self.type == "hk":
            self.table_name = 'hkland_hkshares'
        else:
            raise
        self.inner_code_map = self.get_inner_code_map()
        self.update_fields = ['SecuCode', 'InnerCode', 'SecuAbbr', 'Date', 'Percent', 'ShareNum']

    # def _create_table(self):
    #     """创建爬虫数据库"""
    #     sql = '''
    #      CREATE TABLE IF NOT EXISTS `{}` (
    #       `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
    #       `SecuCode` varchar(16) COLLATE utf8_bin NOT NULL COMMENT '股票交易代码',
    #       `InnerCode` int(11) NOT NULL COMMENT '内部编码',
    #       `SecuAbbr` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '股票简称',
    #       `Date` datetime NOT NULL COMMENT '自然日',
    #       `Percent` decimal(20,4) DEFAULT NULL COMMENT '占A股总股本的比例（%）',
    #       `ShareNum` decimal(20,0) DEFAULT NULL COMMENT '股票数量(股)',
    #       `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
    #       `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    #       PRIMARY KEY (`id`),
    #       UNIQUE KEY `un2` (`InnerCode`,`Date`) USING BTREE
    #     ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='沪/深股通持股记录';
    #     '''.format(self.spider_table)
    #     spider = self._init_pool(self.spider_cfg)
    #     spider.insert(sql)
    #     spider.dispose()

    def get_inner_code_map(self):
        if self.type in ("sh", "sz"):
            sql = 'SELECT SecuCode,InnerCode from SecuMain WHERE SecuCategory in (1, 2) and SecuMarket in (83, 90) and ListedSector in (1, 2, 6, 7);'
        else:
            sql = '''SELECT SecuCode,InnerCode from hk_secumain WHERE SecuCategory in (51, 3, 53, 78) and SecuMarket in (72) and ListedSector in (1, 2, 6, 7);'''
        ret = self.juyuan_conn.query(sql)
        info = {}
        for r in ret:
            key = r.get("SecuCode")
            value = r.get('InnerCode')
            info[key] = value
        return info

    @property
    def post_params(self):
        """构建请求参数"""
        data = {
            '__VIEWSTATE': '/wEPDwUJNjIxMTYzMDAwZGQ79IjpLOM+JXdffc28A8BMMA9+yg==',
            '__VIEWSTATEGENERATOR': 'EC4ACD6F',
            '__EVENTVALIDATION': '/wEdAAdtFULLXu4cXg1Ju23kPkBZVobCVrNyCM2j+bEk3ygqmn1KZjrCXCJtWs9HrcHg6Q64ro36uTSn/Z2SUlkm9HsG7WOv0RDD9teZWjlyl84iRMtpPncyBi1FXkZsaSW6dwqO1N1XNFmfsMXJasjxX85jz8PxJxwgNJLTNVe2Bh/bcg5jDf8=',
            'today': '{}'.format(self.today.strftime("%Y%m%d")),
            'sortBy': 'stockcode',
            'sortDirection': 'asc',
            'alertMsg': '',
            'txtShareholdingDate': '{}'.format(self.check_day),
            'btnSearch': '搜尋',
        }
        return data

    def suffix_process(self, code):
        """对股票代码加后缀"""
        if len(code) == 6:
            if code[0] == '6':
                return code+'.XSHG'
            else:
                return code+'.XSHE'
        else:
            raise

    def _trans_secucode(self, secu_code: str):
        """香港 大陆证券代码转换
        规则: 沪: 60-> 9
             深: 000-> 70, 001-> 71, 002-> 72, 003-> 73, 300-> 77

        FIXME: 科创板  688 在港股无对应的代码
        """
        if self.type == "sh":
            if secu_code.startswith("9"):
                secu_code = "60" + secu_code[1:]
            else:
                logger.warning("{}无对应的大陆编码".format(secu_code))
                raise
        elif self.type == 'sz':
            if secu_code.startswith("70"):
                secu_code = "000" + secu_code[2:]
            elif secu_code.startswith("71"):
                secu_code = "001" + secu_code[2:]
            elif secu_code.startswith("72"):
                secu_code = "002" + secu_code[2:]
            elif secu_code.startswith("73"):
                secu_code = "003" + secu_code[2:]
            elif secu_code.startswith("77"):
                secu_code = "300" + secu_code[2:]
            else:
                logger.warning("{} 无对应的大陆编码".format(secu_code))
                raise
        elif self.type == 'hk':
            # 补上 0
            if len(secu_code) != 5:
                secu_code = "0"*(5-len(secu_code)) + secu_code
        else:
            raise
        return secu_code

    def get_inner_code(self, secu_code):
        ret = self.inner_code_map.get(secu_code)
        if not ret:
            logger.warning("{} 不存在内部编码".format(secu_code))
            raise
        return ret

    def get_secu_name(self, secu_code):
        """网站显示的名称过长 就使用数据库中查询出来的名称 """
        if self.type in ("sh", "sz"):
            sql = 'SELECT ChiNameAbbr from SecuMain WHERE SecuCode ="{}" and SecuCategory in (1, 2) and SecuMarket in (83, 90) and ListedSector in (1, 2, 6, 7);'.format(secu_code)
        else:
            sql = '''SELECT ChiNameAbbr from hk_secumain WHERE SecuCode ="{}" and  SecuCategory in (51, 3, 53, 78) and SecuMarket in (72) and ListedSector in (1, 2, 6, 7);'''.format(secu_code)
        ret = self.juyuan_conn.get(sql).get("ChiNameAbbr")
        return ret

    def check_update(self):
        # 检查网站当日有数据的情况下爬虫数据库是否正常保存到数据
        resp = requests.post(self.url, data=self.post_params)
        if resp.status_code == 200:
            body = resp.text
            doc = html.fromstring(body)
            date = doc.xpath('//*[@id="pnlResult"]/h2/span/text()')[0]
            website_date = re.findall(r"持股日期: (\d{4}/\d{2}/\d{2})", date)[0]
            sql = 'select count(*) as count from {} where Date = "{}";'.format(self.spider_table, website_date)
            count = self.spider_conn.get(sql).get("count")
            if count == 0:
                utils.ding_msg("{} 网站最近更新时间 {} 的爬虫持股数据未更入库".format(self.spider_table, website_date))
            else:
                utils.ding_msg("{} 网站最近更新时间 {} 的爬虫持股数据已更新, 更新数量是 {}".format(self.spider_table, website_date, count))

    def start(self):
        # # (1) 创建爬虫数据库
        # self._create_table()
        # (2) 请求网站获取数据
        resp = requests.post(self.url, data=self.post_params)
        if resp.status_code == 200:
            body = resp.text
            doc = html.fromstring(body)
            date = doc.xpath('//*[@id="pnlResult"]/h2/span/text()')[0]
            date = re.findall(r"持股日期: (\d{4}/\d{2}/\d{2})", date)[0]
            # 与当前参数时间对应的数据时间
            # 举例: 参数时间是 4.26 但是 4.26 无数据更新 之前最近的有数据的日期是 4.25 这里的时间就是 4.25
            logger.info("{}与之对应的之前最近的有数据的一天是 {}".format(self.check_day, date))

            trs = doc.xpath('//*[@id="mutualmarket-result"]/tbody/tr')
            for tr in trs:
                item = {}
                # 股份代码
                secu_code = tr.xpath('./td[1]/div[2]/text()')[0].strip()
                # 聚源内部编码
                _secu_code = self._trans_secucode(secu_code)
                item['InnerCode'] = self.get_inner_code(_secu_code)
                # 股票简称
                secu_name = tr.xpath('./td[2]/div[2]/text()')[0].strip()
                simple_secu_name = self.converter.convert(secu_name)
                if len(simple_secu_name) > 50:
                    simple_secu_name = self.get_secu_name(_secu_code)
                item['SecuAbbr'] = simple_secu_name

                # 时间 即距离当前时间最近的之前有数据的那一天
                item['Date'] = date.replace("/", "-")

                # 於中央結算系統的持股量
                holding = tr.xpath('./td[3]/div[2]/text()')[0]
                if holding:
                    holding = int(holding.replace(',', ''))
                else:
                    holding = 0
                item['ShareNum'] = holding

                # 占股的百分比
                POAShares = tr.xpath('./td[4]/div[2]/text()')
                if POAShares:
                    POAShares = float(POAShares[0].replace('%', ''))
                else:
                    POAShares = float(0)
                item['Percent'] = POAShares

                if self.type == "hk":
                    item['SecuCode'] = _secu_code
                elif self.type in ("sh", "sz"):
                    item['SecuCode'] = self.suffix_process(_secu_code)
                else:
                    raise
                logger.info(item)
                self.spider_conn.table_insert(self.spider_table, item, self.update_fields)
        else:
            raise


def shares_spider_task():
    for market_type in ("sh", "sz", "hk"):
        SharesSpider(market_type).check_update()

    for market_type in ("sh", "sz", "hk"):
        logger.info("{} 爬虫开始运行.".format(market_type))
        # for _offset in range(1, 3):
        # 在凌晨两点的时候只对前一天的数据进行更新
        check_day = datetime.date.today() - datetime.timedelta(days=1)
        logger.info("数据时间是{}".format(check_day))
        SharesSpider(market_type, 1).start()


if __name__ == '__main__':
    pass