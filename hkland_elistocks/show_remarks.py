import datetime
import pprint

from hkland_elistocks.configs import SPIDER_HOST, SPIDER_PORT, SPIDER_USER, SPIDER_PASSWD, SPIDER_DB, JUY_HOST, JUY_PORT, \
    JUY_USER, JUY_PASSWD, JUY_DB, TARGET_HOST, TARGET_PORT, TARGET_USER, TARGET_PASSWD, TARGET_DB
from hkland_elistocks.sql_pool import PyMysqlPoolBase


class ShowTools(object):
    spider_cfg = {
        "host": SPIDER_HOST,
        "port": SPIDER_PORT,
        "user": SPIDER_USER,
        "password": SPIDER_PASSWD,
        "db": SPIDER_DB,
    }

    juyuan_cfg = {
        "host": JUY_HOST,
        "port": JUY_PORT,
        "user": JUY_USER,
        "password": JUY_PASSWD,
        "db": JUY_DB,
    }

    target_cfg = {
        "host": TARGET_HOST,
        "port": TARGET_PORT,
        "user": TARGET_USER,
        "password": TARGET_PASSWD,
        "db": TARGET_DB,
    }

    def init_sql_pool(self, sql_cfg: dict):
        pool = PyMysqlPoolBase(**sql_cfg)
        return pool

    # def show_remarks(self):
    #     sql = 'select distinct(Remarks) from hkex_lgt_change_of_sse_securities_lists;'
    #     spider = self.init_sql_pool(self.target_cfg)
    #     ret = spider.select_all(sql)
    #     for r in ret:
    #         print(r)

    # def show_remark_group_nums(self):
    #     sql = 'select Remarks from hkex_lgt_change_of_sse_securities_lists group by (Remarks) having count(1) >= 3; '
    #     spider = self.init_sql_pool(self.target_cfg)
    #     ret = spider.select_all(sql)
    #     print(len(ret))    # 1: 55; 2: 3, >=3: 39

    # def change_rule(self, change: dict):
    #     if change.get("Ch_ange") == "Addition":
    #         # 加入可进行买入以及卖出的成分股
    #         remark = change.get("Remarks")
    #         if remark == 'Initial list of securities eligible for buy and sell':
    #             record1 = {
    #                 "TradingType": 1,    # 沪股通
    #                 "TargetCategory": 1,  # 状态: 可进行买入以及卖出
    #                 "SecuCode": change.get("SSESCode"),  # 证券代码
    #                 'InDate': change.get("EffectiveDate"),  # 调入生效时间
    #
    #                 # "InnerCode": "",    # 聚源内部编码 待生成
    #                 # "SecuAbbr": '',  # 证券中文简称（待生成）
    #                 # "OutDate": '',  # 调出时间（暂不知）
    #                 # 'Flag': '',  # 当前是否是合资格股票 (暂不知)
    #                 # 'CCASSCode': '',  # 待生成数据
    #                 # 'ParValue': '',  # 面值（待生成）
    #                       }
    #         margin_shortsell = 'This stock will also be added to the List of Eligible SSE Securities for Margin Trading and the List of Eligible SSE Securities for Short Selling'
    #         if margin_shortsell in remark:
    #             # 加入可进行保证金交易的列表
    #             record3 = {
    #                 "TradingType": 1,  # 沪股通
    #                 "TargetCategory": 3,  # 状态: 可进行保证金交易
    #                 "SecuCode": change.get("SSESCode"),  # 证券代码
    #                 'InDate': change.get("EffectiveDate"),  # 调入生效时间
    #             }
    #             # 加入可进行担保卖空的列表
    #             record4 = {
    #                 "TradingType": 1,  # 沪股通
    #                 "TargetCategory": 4,  # 状态: 可进行担保卖空
    #                 "SecuCode": change.get("SSESCode"),  # 证券代码
    #                 'InDate': change.get("EffectiveDate"),  # 调入生效时间
    #             }

    def show_changes_with_a_num(self, num):
        sql = 'select SecuCode from lc_shscelistocks where TradingType = 1 group by SecuCode  having count(1) = {};'.format(num)
        juyuan = self.init_sql_pool(self.juyuan_cfg)
        ret = juyuan.select_all(sql)
        ret = [r.get("SecuCode") for r in ret]
        return ret

    def show_code_spider_records(self, code):
        sql = 'select EffectiveDate, Ch_ange, Remarks from hkex_lgt_change_of_sse_securities_lists where SSESCode = "{}" order by EffectiveDate;'.format(code)
        spider = self.init_sql_pool(self.target_cfg)
        ret = spider.select_all(sql)
        return ret

    def show_code_juyuan_records(self, code):
        # sql = 'select *  from lc_shscelistocks where SecuCode = {}; '.format(code)
        sql = 'select TargetCategory, InDate, OutDate, Flag from lc_shscelistocks where SecuCode = {} and TradingType = 1 order by InDate;'.format(code)
        juyuan = self.init_sql_pool(self.juyuan_cfg)
        ret = juyuan.select_all(sql)
        return ret

    def _add_ok_codes(self, code):
        with open("check_right.txt", "a+") as f:
            f.write(code+"\n")

    def _add_no_codes(self, code):
        with open("check_wrong.txt", "a+") as f:
            f.write(code + "\n")

    def pre_process_spider_changes(self, changes):
        # 枚举变化
        infos = []
        for change in changes:
            if change.get("Ch_ange") == "Addition":
                record = {'TargetCategory': 1, 'InDate': change.get("EffectiveDate")}
            elif change.get("Ch_ange") == "Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)":
                record = {'TargetCategory': 2, 'InDate': change.get("EffectiveDate")}

    def check_by_human(self):
        num = int(input("聚源中记录数量为 num 的 code, 请输入 num: \n>> "))
        print("输入的 num 是 {}".format(num))
        _code_with_change_8 = self.show_changes_with_a_num(num)
        print(_code_with_change_8)    # ['600094', '600433', '600770']
        for code in _code_with_change_8:
            juyuan_records = self.show_code_juyuan_records(code)
            print(code)

            print("聚源的记录是: ")
            for juyuan_record in juyuan_records:
                print(juyuan_record)

            print("spider 的记录是: ")
            spider_records = self.show_code_spider_records(code)
            for spider_record in spider_records:
                print(spider_record)

            _is_ok = int(input("请输入你的核对结果 1: 一致; 2: 有误 \n >> "))
            if _is_ok == 1:
                self._add_ok_codes(code)
            else:
                self._add_no_codes(code)


if __name__ == "__main__":
    show = ShowTools()
    show.check_by_human()

    # show.show_code_spider_records("600094")
    # show.show_code_juyuan_records("600094")

    # info = [
    #     {
    #         "code": '',
    #         'date': datetime.date(2014, 11, 17),
    #         'stats1': 1,    #
    #         'stats2': 0,
    #         'stats3': 0,
    #         'stats4': 0,
    #         'stats5': 0,
    #     },
    #
    # ]

    # show.show_changes_with_a_num(9)    # [{'SecuCode': '600094'}]
    # show.show_remarks()
    # show.show_remark_group_nums()


'''
           ID: 475958066653
   TradingType: 1
TargetCategory: 1
     InnerCode: 1120
      SecuCode: 600000
      SecuAbbr: 浦發銀行
        InDate: 2014-11-17 00:00:00
       OutDate: NULL
          Flag: 1
     CCASSCode: 90000
      ParValue: 1
    UpdateTime: 2015-03-12 18:00:12
          JSID: 479498411998
1 row in set (0.02 sec)
'''


'''沪港通合资格股份
CREATE TABLE `lc_shscelistocks` (
  `ID` bigint(20) NOT NULL COMMENT 'ID',
  `TradingType` int(11) NOT NULL COMMENT '交易方向',      # 1-沪股通，2-港股通（沪）
  `TargetCategory` int(11) NOT NULL COMMENT '标的类别',   # 1-可买入及卖出，2-只可卖出，3-可进行保证金交易，4-可进行担保卖空，5-触发持股比例限制暂停买入。
  `InnerCode` int(11) NOT NULL COMMENT '证券内部编码',     # 证券内部编码（InnerCode）：当TradingType=1时，与“证券主表（SecuMain）”中的“内部编码（InnerCode）”关联，得到股票的交易代码、简称等；当TradingType=2时，与“港股证券主表（HK_SecuMain）”中的“内部编码（InnerCode）”关联，得到股票的交易代码、简称等；
  `SecuCode` varchar(50) DEFAULT NULL COMMENT '证券代码',
  `SecuAbbr` varchar(50) DEFAULT NULL COMMENT '证券简称',
  `InDate` datetime NOT NULL COMMENT '调入日期',
  `OutDate` datetime DEFAULT NULL COMMENT '调出日期',
  `Flag` int(11) DEFAULT NULL COMMENT '资讯级别',          # 该字段固定以下常量：1-是；2-否 应该是标识当前的状态: 是否是合资格股票
  `CCASSCode` varchar(50) DEFAULT NULL COMMENT 'CCASS股份编码',    # CCASS Stock Code
  `ParValue` varchar(50) DEFAULT NULL COMMENT '面值(人民币)',  # 面值
  `UpdateTime` datetime NOT NULL COMMENT '更新时间',
  `JSID` bigint(20) NOT NULL COMMENT 'JSID',
  UNIQUE KEY `IX_LC_SHSCEliStocks` (`InnerCode`,`TradingType`,`TargetCategory`,`InDate`),
  UNIQUE KEY `IX_LC_SHSCEliStocks_ID` (`ID`),
  UNIQUE KEY `IX_LC_SHSCEliStocks_JSID` (`JSID`)
) ENGINE=InnoDB DEFAULT CHARSET=gbk;
'''
