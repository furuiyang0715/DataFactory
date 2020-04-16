# -*- coding: utf-8 -*-

import datetime
import json
import logging
import re
import sys
import time
import traceback

import requests
import schedule

sys.path.append("./../")

from hkland_toptrade.base_spider import BaseSpider
from hkland_toptrade.configs import LOCAL

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EMLgttop10tradedsharesspiderSpider(BaseSpider):
    """十大成交股 东财数据源 """
    def __init__(self, day: str):
        self.headers = {
            'Referer': 'http://data.eastmoney.com/hsgt/top10.html',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
        }
        self.day = day    # datetime.datetime.strftime("%Y-%m-%d")
        self.url = 'http://data.eastmoney.com/hsgt/top10/{}.html'.format(day)
        self.table_name = 'hkland_toptrade'
        self.tool_table_name = 'base_table_updatetime'

    def _get_inner_code_map(self, market_type):
        """https://dd.gildata.com/#/tableShow/27/column///
           https://dd.gildata.com/#/tableShow/718/column///
        """
        juyuan = self._init_pool(self.juyuan_cfg)
        if market_type in ("sh", "sz"):
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

    def refresh_update_time(self):
        product = self._init_pool(self.product_cfg)
        sql = '''select max(UPDATETIMEJZ) as max_dt from {}; '''.format(self.table_name)
        max_dt = product.select_one(sql).get("max_dt")
        logger.info("最新的更新时间是{}".format(max_dt))

        refresh_sql = '''replace into {} (id,TableName, LastUpdateTime,IsValid) values (10, "hkland_toptrade", '{}', 1); 
        '''.format(self.tool_table_name, max_dt)
        count = product.update(refresh_sql)
        logger.info(count)   # 1 首次插入 2 替换插入
        product.dispose()

    def _start(self):
        if LOCAL:
            self._create_table()

        resp = requests.get(self.url, headers=self.headers)
        if resp.status_code == 200:
            body = resp.text
            # 沪股通十大成交股
            data1 = re.findall('var DATA1 = (.*);', body)[0]
            # 深股通十大成交股
            data2 = re.findall('var DATA2 = (.*);', body)[0]
            # 港股通(沪)十大成交股
            data3 = re.findall('var DATA3 = (.*);', body)[0]
            # 港股通(深)十大成交股
            data4 = re.findall('var DATA4 = (.*);', body)[0]

            sh_innercode_map = self._get_inner_code_map("sh")
            sz_innercode_map = self._get_inner_code_map("sz")
            hk_innercode_map = self._get_inner_code_map("hk")

            for data in [data1, data2, data3, data4]:
                data = json.loads(data)
                print(data)
                top_datas = data.get("data")
                print(top_datas)
                for top_data in top_datas:
                    item = dict()
                    item['Date'] = self.day   # 时间
                    secu_code = top_data.get("Code")
                    item['SecuCode'] = secu_code  # 证券代码
                    item['SecuAbbr'] = top_data.get("Name")   # 证券简称
                    item['Close'] = top_data.get('Close')  # 收盘价
                    item['ChangePercent'] = top_data.get('ChangePercent')  # 涨跌幅
                    item['CMFID'] = 1  # 兼容之前的程序 写死
                    item['CMFTime'] = datetime.datetime.now()   # 兼容和之前的程序 用当前的时间代替
                    # '类别代码:GGh: 港股通(沪), GGs: 港股通(深), HG: 沪股通, SG: 深股通',
                    if top_data['MarketType'] == 1.0:
                        item['CategoryCode'] = 'HG'
                        # item['Category'] = '沪股通'
                        # 净买额
                        item['TJME'] = top_data['HGTJME']
                        # 买入金额
                        item['TMRJE'] = top_data['HGTMRJE']
                        # # 卖出金额
                        # item['TMCJE'] = top_data['HGTMCJE']
                        # 成交金额
                        item['TCJJE'] = top_data['HGTCJJE']
                        item['InnerCode'] = sh_innercode_map.get(secu_code)

                    elif top_data['MarketType'] == 2.0:
                        item['CategoryCode'] = 'GGh'
                        # item['Category'] = '港股通(沪)'
                        # 港股通(沪)净买额(港元）
                        item['TJME'] = top_data['GGTHJME']
                        # 港股通(沪)买入金额(港元）
                        item['TMRJE'] = top_data['GGTHMRJE']
                        # # 港股通(沪)卖出金额(港元）
                        # item['TMCJE'] = top_data['GGTHMCJE']
                        # 港股通(沪)成交金额(港元）
                        item['TCJJE'] = top_data['GGTHCJJE']
                        item['InnerCode'] = hk_innercode_map.get(secu_code)

                    elif top_data['MarketType'] == 3.0:
                        item['CategoryCode'] = 'SG'
                        # item['Category'] = '深股通'
                        # 净买额
                        item['TJME'] = top_data['SGTJME']
                        # 买入金额
                        item['TMRJE'] = top_data['SGTMRJE']
                        # # 卖出金额
                        # item['TMCJE'] = top_data['SGTMCJE']
                        # 成交金额
                        item['TCJJE'] = top_data['SGTCJJE']
                        item['InnerCode'] = sz_innercode_map.get(secu_code)

                    elif top_data['MarketType'] == 4.0:
                        item['CategoryCode'] = 'GGs'
                        # item['Category'] = '港股通(深)'
                        # 港股通(沪)净买额(港元）
                        item['TJME'] = top_data['GGTSJME']
                        # 港股通(沪)买入金额(港元）
                        item['TMRJE'] = top_data['GGTSMRJE']
                        # # 港股通(沪)卖出金额(港元）
                        # item['TMCJE'] = top_data['GGTSMCJE']
                        # 港股通(沪)成交金额(港元）
                        item['TCJJE'] = top_data['GGTSCJJE']
                        item['InnerCode'] = hk_innercode_map.get(secu_code)

                    else:
                        raise
                    # print(item)
                    update_fields = ['Date', 'SecuCode', 'InnerCode', 'SecuAbbr', 'Close', 'ChangePercent',
                                     'TJME', 'TMRJE', 'TCJJE', 'CategoryCode']
                    self._save(item, self.table_name, update_fields)

        self.refresh_update_time()

    def _save(self, to_insert, table, update_fields: list):
        product = self._init_pool(self.product_cfg)
        try:
            insert_sql, values = self.contract_sql(to_insert, table, update_fields)
            count = product.insert(insert_sql, values)
        except:
            traceback.print_exc()
            logger.warning("失败")
            count = None
        else:
            if count:
                logger.info("更入新数据 {}".format(to_insert))
            else:
                pass
                # logger.info("{} 数据已插入 ".format(to_insert))
        finally:
            product.dispose()
        return count

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
        product = self._init_pool(self.product_cfg)
        product.insert(sql)
        product.dispose()

    def start(self):
        try:
            self._start()
        except:
            traceback.print_exc()


def schedule_task():
    t_day = datetime.datetime.today()

    start_time = datetime.datetime(t_day.year, t_day.month, t_day.day, 9, 0, 0)
    end_time = datetime.datetime(t_day.year, t_day.month, t_day.day, 15, 0, 0)

    if (t_day >= start_time and t_day <= end_time):
        logger.warning("在盘中不更新数据")
        return

    for i in range(1):
        # FIXME 只能获取最近一天的数据
        day = t_day - datetime.timedelta(days=i)
        day_str = day.strftime("%Y-%m-%d")
        print(day_str)  # 如果当前还未出 十大成交股数据 返回空列表
        top10 = EMLgttop10tradedsharesspiderSpider(day_str)
        top10.start()


def main():
    schedule_task()

    schedule.every(10).minutes.do(schedule_task)

    while True:
        print("当前调度系统中的任务列表是{}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":

    main()


'''
docker build -f Dockerfile_top -t registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_toptrade:v1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_toptrade:v1 
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_toptrade:v1 


# remote 
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name toptrade \
--env LOCAL=0 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_toptrade:v1 

# local
sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name toptrade \
--env LOCAL=1 \
registry.cn-shenzhen.aliyuncs.com/jzdev/jzdata/hkland_toptrade:v1 
'''