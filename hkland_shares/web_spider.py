import base64
import datetime
import hashlib
import hmac
import json
import logging
import os
import re
import sys
import time
import traceback
import urllib.parse
import requests
import opencc
from apscheduler.schedulers.blocking import BlockingScheduler
from lxml import html

sys.path.append("./../")
from hkland_shares.configs import (SPIDER_MYSQL_HOST, SPIDER_MYSQL_PORT, SPIDER_MYSQL_USER, SPIDER_MYSQL_PASSWORD,
                                   SPIDER_MYSQL_DB, PRODUCT_MYSQL_HOST, PRODUCT_MYSQL_PORT, PRODUCT_MYSQL_USER,
                                   PRODUCT_MYSQL_PASSWORD, PRODUCT_MYSQL_DB, JUY_HOST, JUY_PORT, JUY_USER, JUY_PASSWD,
                                   JUY_DB, DC_HOST, DC_PORT, DC_USER, DC_PASSWD, DC_DB, SECRET, TOKEN)
from hkland_shares.sql_pool import PyMysqlPoolBase

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

now = lambda: time.time()


class HoldShares(object):
    """滬股通及深股通持股紀錄按日查詢"""
    spider_cfg = {    # 爬虫库
        "host": SPIDER_MYSQL_HOST,
        "port": SPIDER_MYSQL_PORT,
        "user": SPIDER_MYSQL_USER,
        "password": SPIDER_MYSQL_PASSWORD,
        "db": SPIDER_MYSQL_DB,
    }

    product_cfg = {    # 正式库
        "host": PRODUCT_MYSQL_HOST,
        "port": PRODUCT_MYSQL_PORT,
        "user": PRODUCT_MYSQL_USER,
        "password": PRODUCT_MYSQL_PASSWORD,
        "db": PRODUCT_MYSQL_DB,
    }

    juyuan_cfg = {    # 聚源数据库
        "host": JUY_HOST,
        "port": JUY_PORT,
        "user": JUY_USER,
        "password": JUY_PASSWD,
        "db": JUY_DB,
    }

    dc_cfg = {        # 数据中心库
        "host": DC_HOST,
        "port": DC_PORT,
        "user": DC_USER,
        "password": DC_PASSWD,
        "db": DC_DB,
    }

    def __init__(self, type, offset=1):
        """
        默认只更新之前一天的记录
        """
        self.type = type
        self.url = 'https://www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t={}'.format(type)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
        }
        self.today = datetime.datetime.combine(datetime.date.today(), datetime.time.min)
        self.offset = offset
        # FIXME 注意: offset 决定的是查询哪一天的记录 且站在当前时间点只能查询之前一天往前的记录
        self.check_day = (datetime.date.today() - datetime.timedelta(days=self.offset)).strftime("%Y/%m/%d")
        self.converter = opencc.OpenCC('t2s')  # 中文繁体转简体
        _type_map = {
            'sh': '沪股通',
            'sz': '深股通',
            'hk': '港股通',
        }

        _market_map = {
            "sh": 83,
            "sz": 90,
            'hk': 72,

        }
        self.market = _market_map.get(self.type)

        self.type_name = _type_map.get(self.type)
        _percent_comment_map = {
            'sh': '占于上交所上市及交易的A股总数的百分比(%)',
            'sz': '占于深交所上市及交易的A股总数的百分比(%)',
            'hk': '占已发行股份的百分比(%)',
        }
        self.percent_comment = _percent_comment_map.get(self.type)

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

    def _init_pool(self, cfg):
        pool = PyMysqlPoolBase(**cfg)
        return pool

    def _create_table(self):
        """创建爬虫数据库"""
        sql = '''
         CREATE TABLE IF NOT EXISTS `{}` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `SecuCode` varchar(16) COLLATE utf8_bin NOT NULL COMMENT '股票交易代码',
          `InnerCode` int(11) NOT NULL COMMENT '内部编码',
          `SecuAbbr` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '股票简称',
          `Date` datetime NOT NULL COMMENT '自然日',
          `Percent` decimal(20,4) DEFAULT NULL COMMENT '占A股总股本的比例（%）',
          `ShareNum` decimal(20,0) DEFAULT NULL COMMENT '股票数量(股)',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `un2` (`InnerCode`,`Date`) USING BTREE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='沪/深股通持股记录'; 
        '''.format(self.spider_table)
        spider = self._init_pool(self.spider_cfg)
        spider.insert(sql)
        spider.dispose()

    def contract_sql(self, to_insert: dict, table: str, update_fields: list):
        ks = []
        vs = []
        for k in to_insert:
            ks.append(k)
            vs.append(to_insert.get(k))
        fields_str = "(" + ",".join(ks) + ")"
        values_str = "(" + "%s," * (len(vs) - 1) + "%s" + ")"
        base_sql = '''INSERT INTO `{}` '''.format(table) + fields_str + ''' values ''' + values_str
        on_update_sql = ''' ON DUPLICATE KEY UPDATE '''
        update_vs = []
        for update_field in update_fields:
            on_update_sql += '{}=%s,'.format(update_field)
            update_vs.append(to_insert.get(update_field))
        on_update_sql = on_update_sql.rstrip(",")
        sql = base_sql + on_update_sql + """;"""
        vs.extend(update_vs)
        return sql, tuple(vs)

    def _save(self, sql_pool, to_insert, table, update_fields):
        try:
            insert_sql, values = self.contract_sql(to_insert, table, update_fields)
            count = sql_pool.insert(insert_sql, values)
        except:
            traceback.print_exc()
            logger.warning("失败")
        else:
            if count == 1:    # 插入新数据的时候结果为 1
                logger.info("插入新数据 {}".format(to_insert))

            elif count == 2:
                logger.info("刷新数据 {}".format(to_insert))

            else:   # 数据已经存在的时候结果为 0
                # logger.info(count)
                # logger.info("已有数据 {} ".format(to_insert))
                pass

            sql_pool.end()
            return count

    def get_inner_code_map(self):
        """https://dd.gildata.com/#/tableShow/27/column///
           https://dd.gildata.com/#/tableShow/718/column///
        """
        juyuan = self._init_pool(self.juyuan_cfg)
        if self.type in ("sh", "sz"):
            sql = 'SELECT SecuCode,InnerCode from SecuMain WHERE SecuCategory in (1, 2) and SecuMarket in (83, 90) and ListedSector in (1, 2, 6, 7);'
        else:
            sql = '''SELECT SecuCode,InnerCode from hk_secumain WHERE SecuCategory in (51, 3, 53, 78) and SecuMarket in (72) and ListedSector in (1, 2, 6, 7);'''
        ret = juyuan.select_all(sql)
        juyuan.dispose()
        info = {}
        for r in ret:
            key = r.get("SecuCode")
            value = r.get('InnerCode')
            info[key] = value
        return info

    def suffix_process(self, code):
        """对代码进行加减后缀"""
        if len(code) == 6:
            if code[0] == '6':
                return code+'.XSHG'
            else:
                return code+'.XSHE'
        else:
            raise

    def ding(self, msg):
        def get_url():
            timestamp = str(round(time.time() * 1000))
            secret_enc = SECRET.encode('utf-8')
            string_to_sign = '{}\n{}'.format(timestamp, SECRET)
            string_to_sign_enc = string_to_sign.encode('utf-8')
            hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
            sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
            url = 'https://oapi.dingtalk.com/robot/send?access_token={}&timestamp={}&sign={}'.format(
                TOKEN, timestamp, sign)
            return url

        url = get_url()
        header = {
            "Content-Type": "application/json",
            "Charset": "UTF-8"
        }
        message = {
            "msgtype": "text",
            "text": {
                "content": "{}@15626046299".format(msg)
            },
            "at": {
                "atMobiles": [
                    "15626046299",
                ],
                "isAtAll": False
            }
        }
        message_json = json.dumps(message)
        resp = requests.post(url=url, data=message_json, headers=header)
        if resp.status_code == 200:
            pass
        else:
            logger.warning("钉钉消息发送失败")

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
        juyuan = self._init_pool(self.juyuan_cfg)
        if self.type in ("sh", "sz"):
            sql = 'SELECT ChiNameAbbr from SecuMain WHERE SecuCode ="{}" and SecuCategory in (1, 2) and SecuMarket in (83, 90) and ListedSector in (1, 2, 6, 7);'.format(secu_code)
        else:
            sql = '''SELECT ChiNameAbbr from hk_secumain WHERE SecuCode ="{}" and  SecuCategory in (51, 3, 53, 78) and SecuMarket in (72) and ListedSector in (1, 2, 6, 7);'''.format(secu_code)
        ret = juyuan.select_one(sql).get("ChiNameAbbr")
        juyuan.dispose()
        return ret

    def get_date(self):
        resp = requests.post(self.url, data=self.post_params)
        if resp.status_code == 200:
            body = resp.text
            doc = html.fromstring(body)
            date = doc.xpath('//*[@id="pnlResult"]/h2/span/text()')[0]
            date = re.findall(r"持股日期: (\d{4}/\d{2}/\d{2})", date)[0]
            return date

    def select_spider_in_dt(self, dt):
        cli = self._init_pool(self.spider_cfg)
        sql = 'select count(*) as count from {} where Date = "{}";'.format(self.spider_table, dt)
        # print(sql)
        ret = cli.select_one(sql).get("count")
        return ret

    def check_update(self):
        date = self.get_date()
        count = self.select_spider_in_dt(date)
        # print(count)
        if count == 0:
            self.ding("{} 网站最近更新时间 {} 的爬虫持股数据未更入库".format(self.spider_table, date))
        else:
            self.ding("{} 网站最近更新时间 {} 的爬虫持股数据已更新, 更新数量是 {}".format(self.spider_table, date, count))

    def start(self):
        for i in range(5):
            try:
                self._start()
            except Exception:
                logger.info("{} >> crawl error..".format(i))
                time.sleep(20)
            else:
                logger.info("{} >> crawl ok.. ".format(i))
                break

    def _start(self):
        # (1) 创建爬虫数据库
        self._create_table()
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
            update_fields = ['SecuCode', 'InnerCode', 'SecuAbbr', 'Date', 'Percent', 'ShareNum']
            spider = self._init_pool(self.spider_cfg)
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

                self._save(spider, item, self.spider_table, update_fields)

            try:
                spider.dispose()
            except:
                pass
        else:
            raise


def mysql_task():
    try:
        for _type in ("sh", "sz", "hk"):
            h = HoldShares(_type)
            # 默认 offset 为 1 的情况下
            h.check_update()
    except:
        pass


def spider_task():
    retry = 1
    while True:
        try:
            logger.info('第{}次尝试运行'.format(retry))
            t1 = now()
            for _type in ("sh", "sz", "hk"):
                logger.info("{} 爬虫开始运行.".format(_type))
                for _offset in range(1, 3):
                    _check_day = datetime.date.today() - datetime.timedelta(days=_offset)
                    logger.info("数据时间是{}".format(_check_day))
                    h = HoldShares(_type, _offset)
                    h.start()
                    logger.info("当前耗时{} s".format(now() - t1))
        except Exception as e:
            traceback.print_exc()
            logger.warning('第 {} 次运行失败, 原因是 {}'.format(retry, e))
            retry += 1
            while retry > 3:
                raise
            time.sleep(20)
        else:
            logger.info("spider task ok.")
            return


# def spider_task():
#     logger.info('我是被测试执行的任务')
#     raise Exception("我在执行的过程中出错了 ")


if __name__ == '__main__':
    scheduler = BlockingScheduler()
    # 确保重启时可以执行一次
    spider_task()
    mysql_task()
    scheduler.add_job(spider_task, 'cron', hour='0-3, 3-6', minute='0, 20, 40')
    scheduler.add_job(mysql_task, 'cron', hour='3')
    logger.info('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception as e:
        logger.info(f"本次任务执行出错{e}")
        sys.exit(0)


# if __name__ == "__main__":
#
#     spider_task()
#
#     mysql_task()


# 检查沪股中的浦发银行
# select * from holding_shares_sh where SecuCode = '600000.XSHG' order by Date;
# select * from hkland_shares where SecuCode = '600000.XSHG' order by Date desc limit 10;

# 检查深股中的平安银行
# select * from holding_shares_sz where SecuCode = '000001.XSHE' order by Date;
# select * from hkland_shares where SecuCode = '000001.XSHE' order by Date desc limit 10;

# 检查港股中的长和
# select * from holding_shares_hk where SecuCode = '00001' order by Date;

# 注: 如果需要的不是目前的数据 请修改 offset

# 部署
'''
docker build -f Dockerfile_webspider -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/shares_webspider:v1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/shares_webspider:v1 
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/shares_webspider:v1 
# remote 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name shares_spider \
--env LOCAL=0 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/shares_webspider:v1  

# local
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name shares_spider \
--env LOCAL=1 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/shares_webspider:v1   

'''
