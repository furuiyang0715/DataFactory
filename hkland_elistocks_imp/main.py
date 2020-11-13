import copy
import datetime
import functools
import os
import pprint
import sys
import time
import traceback

import schedule

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)

from hkland_elistocks_imp.const import records_sh, records_sz
from hkland_elistocks_imp.configs import LOCAL, SQL_DEAL
from hkland_elistocks_imp.base import logger, BaseSpider
from hkland_elistocks_imp.ganerate_hklands import DailyUpdate


def catch_exceptions(cancel_on_failure=False):
    """
    装饰器, 对定时任务中的异常进行捕获, 并决定是否在异常发生时取消任务
    :param cancel_on_failure:
    :return:
    """

    def catch_exceptions_decorator(job_func):
        @functools.wraps(job_func)
        def wrapper(*args, **kwargs):
            try:
                return job_func(*args, **kwargs)
            except:
                logger.warning(traceback.format_exc())
                # sentry.captureException(exc_info=True)
                if cancel_on_failure:
                    logger.warning("异常, 任务结束, {}".format(schedule.CancelJob))
                    schedule.cancel_job(job_func)
                    return schedule.CancelJob

        return wrapper

    return catch_exceptions_decorator


class OriginChecker(BaseSpider):
    def __init__(self):
        super(OriginChecker, self).__init__()
        self.sql_deal = SQL_DEAL
        self.tool_table_name = 'base_table_updatetime'

    def refresh_update_time(self):
        """在工具表中刷新更新时间
        'hkland_hgelistocks' # 沪港通合资格股
        'hkland_sgelistocks' # 深港通合资格股

        3 | hkland_hgelistocks      | 2020-06-16 23:17:39 |       1
        8 | hkland_sgelistocks      | 2020-06-16 23:18:17 |       1

        replace into base_table_updatetime (id,TableName, LastUpdateTime,IsValid) values (3, 'hkland_hgelistocks', '{}', 1);
        replace into base_table_updatetime (id,TableName, LastUpdateTime,IsValid) values (8, 'hkland_sgelistocks', '{}', 1);

        """
        self._product_init()
        for table_name, num in {"hkland_hgelistocks": 3, "hkland_sgelistocks": 8}.items():
            sql = '''select max(UPDATETIMEJZ) as max_dt from {};'''.format(table_name)
            max_dt = self.product_client.select_one(sql).get("max_dt")
            logger.info("最新的更新时间是{}".format(max_dt))
            refresh_sql = '''replace into {} (id,TableName, LastUpdateTime,IsValid) values ({}, '{}', '{}', 1);
            '''.format(self.tool_table_name, num, table_name, max_dt)
            print(refresh_sql)
            self.product_client.update(refresh_sql)
            self.product_client.end()

    def get_distinct_spider_udpate_time(self, table_name):
        self._spider_init()
        sql = '''select distinct(Time) from {};'''.format(table_name)
        ret = self.spider_client.select_all(sql)
        return ret

    def select_onetime_records(self, table_name, onetime: datetime.datetime):
        """获取某一个时间点的记录"""
        self._spider_init()
        sql = '''select * from {} where Time = '{}'; '''.format(table_name, onetime)
        ret = self.spider_client.select_all(sql)
        return ret

    def process_sz_changes(self, changes):
        change_removal = 'Removal'
        change_removal_more = 'Remove from List of Eligible SZSE Securities for Margin Trading and List of Eligible SZSE Securities for Short Selling'

        change_addition = 'Addition'
        change_addition_more = 'Addition to List of Eligible SZSE Securities for Margin Trading and List of Eligible SZSE Securities for Short Selling'
        change_addition_less = 'Addition (from List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only))'

        change_transfer = 'Transfer to List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)'
        change_recover = 'Addition (from List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only))'

        change_buyorders_resumed = 'Buy orders resumed'
        change_buyorders_suspended = 'Buy orders suspended'

        addition_sentence = 'This stock will also be added to the List of Eligible SZSE Securities for Margin Trading and the List of Eligible SZSE Securities for Short Selling'
        recover_sentence = 'This stock will also be added to the List of Eligible SZSE Securities for Margin Trading and the List of Eligible SZSE Securities for Short Selling as it is also included in SZSE stock list for margin trading and shortselling.'
        remove_sentence = 'This stock will also be removed from the List of Eligible SZSE Securities for Margin Trading and the List of Eligible SZSE Securities for Short Selling.'
        rename_sentence = 'SZSE Stock Code and Stock Name are changed'

        self.process("sz",
                     changes,
                     change_removal,
                     change_removal_more,
                     change_addition,
                     change_addition_more,
                     change_addition_less,
                     change_transfer,
                     change_recover,
                     change_buyorders_resumed,
                     change_buyorders_suspended,
                     addition_sentence,
                     recover_sentence,
                     remove_sentence,
                     rename_sentence,
                     )

    def process_sh_changes(self, changes):
        # 标的类别(TargetCategory): 1-可买入及卖出，2-只可卖出，3-可进行保证金交易，4-可进行担保卖空，5-触发持股比例限制暂停买入。
        change_add1 = 'Addition'  # 該Ａ股將納入中華通證券
        # 在加入 1 的时候同时加入 3 4
        sentence_add34 = 'This stock will also be added to the List of Eligible SSE Securities for Margin Trading and the List of Eligible SSE Securities for Short Selling'

        # 加入可進行保證金交易的合資格上交所證券名單及可賣空的合資格滬股通證券名單
        change_add34 = 'Addition to List of Eligible SSE Securities for Margin Trading and List of Eligible SSE Securities for Short Selling'

        # 移至滬股通特別證券/中華通特別證券名單 (只可賣出)
        change_add2 = 'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)'
        # 移入 2 时，一般要移出 1, 在有特别说明时移出  3 4
        sentence_rm34 = 'This stock will also be removed from the List of Eligible SZSE Securities for Margin Trading and the List of Eligible SZSE Securities for Short Selling.'

        # 移除, 从 2 中移除
        change_rvl2 = 'Removal'

        # 加入(由滬股通特別證券/中華通特別證券名單 (只可賣出))
        # 意思为移出 2 加入 1
        change_rm2 = "Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))"
        # 在移出 2 的时候注明是否加入 3 4
        # sentence_add34 = 'This stock will also be added to the List of Eligible SSE Securities for Margin Trading and the List of Eligible SSE Securities for Short Selling'

        # 已暫停買入
        change_suspended = 'Buy orders suspended'

        # 已恢復買入
        change_resumed = 'Buy orders resumed'

        self.process("sh",
                     changes,
                     change_add1,
                     sentence_add34,
                     change_add34,
                     change_add2,
                     sentence_rm34,
                     change_rvl2,
                     change_rm2,
                     change_suspended,
                     change_resumed,
                     )

    def process(self, flag,
                changes,
                change_add1,
                sentence_add34,
                change_add34,
                change_add2,
                sentence_rm34,
                change_rvl2,
                change_rm2,
                change_suspended,
                change_resumed,
                ):

        add_1 = []
        add_34 = []
        add_134 = []
        add_2 = []
        rm_134 = []
        rm_1 = []
        rvl_2 = []
        rm_2 = []

        for change in changes:
            _change, _remarks, secu_code = change.get('Ch_ange'), change.get("Remarks"), change.get("SSESCode")
            _effectivedate = change.get("EffectiveDate")

            if _change == change_add1:
                if sentence_add34 in _remarks:
                    add_134.append((secu_code, _effectivedate))
                else:
                    add_1.append((secu_code, _effectivedate))

            elif _change == change_add34:
                add_34.append((secu_code, _effectivedate))

            elif _change == change_add2:
                add_2.append((secu_code, _effectivedate))
                if sentence_rm34 in _remarks:
                    rm_134.append((secu_code, _effectivedate))
                else:
                    rm_1.append((secu_code, _effectivedate))

            elif _change == change_rvl2:
                rvl_2.append((secu_code, _effectivedate))

            elif _change == change_rm2:
                rm_2.append((secu_code, _effectivedate))
                add_1.append((secu_code, _effectivedate))
                if sentence_add34 in _remarks:
                    add_34.append((secu_code, _effectivedate))

        if not self.sql_deal:
            return

        # 对相应的项目进行增删改
        # ID  TradingType TargetCategory InnerCode SecuCode SecuAbbr InDate OutDate Flag CCASSCode
        # ParValue CREATETIMEJZ UPDATETIMEJZ CMFID CMFTime
        self._product_init()
        fields = ['TradingType', 'TargetCategory', 'InnerCode', 'SecuCode',
                  # 'SecuAbbr',
                  'InDate', 'OutDate', 'Flag']
        trading_type = 1 if flag == "sh" else 3
        table_name = 'hkland_hgelistocks' if trading_type == 1 else 'hkland_sgelistocks'

        for secu_code, _date in add_1:
            inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
            item = {'TradingType': trading_type,
                    'TargetCategory': 1,
                    'InnerCode': inner_code,
                    'SecuCode': secu_code,
                    'SecuAbbr': secu_abbr,
                    'InDate': _date,
                    'Flag': 1,
                    }
            self._save(self.product_client, item, table_name, fields)

        for secu_code, _date in add_134:
            inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
            base_item = {
                'TradingType': trading_type, 'InnerCode': inner_code, 'SecuCode': secu_code,
                'SecuAbbr': secu_abbr, 'InDate': _date, 'Flag': 1,
            }
            item1, item3, item4 = copy.deepcopy(base_item), copy.deepcopy(base_item), copy.deepcopy(base_item)
            item1.update({'TargetCategory': 1})
            item3.update({'TargetCategory': 3})
            item4.update({'TargetCategory': 4})
            self._batch_save(self.product_client, [item1, item3, item4], table_name, fields)

        # for item in recover_1:
        #     print(item)
        #     #  结束 2
        #     secu_code, _date = item
        #     in_date_item = self.get_indate_data(trading_type, table_name, 2, secu_code)
        #     if in_date_item:
        #         in_date_item.update({"Flag": 2, "OutDate": _date})
        #         r1 = self._save(self.product_client, in_date_item, table_name, fields)
        #         print("***** ", r1)
        #         # （恢复）增加 1
        #         inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
        #         item = {'TradingType': trading_type,
        #                 'TargetCategory': 1,
        #                 'InnerCode': inner_code,
        #                 'SecuCode': secu_code,
        #                 'SecuAbbr': secu_abbr,
        #                 'InDate': _date,
        #                 'Flag': 1,
        #                 }
        #         r2 = self._save(self.product_client, item, table_name, fields)
        #         print("##### ", r2)
        #
        # for item in recover_134:
        #     print(item)
        #     # 结束 2
        #     secu_code, _date = item
        #     in_date_item = self.get_indate_data(trading_type, table_name, 2, secu_code)
        #     if in_date_item:
        #         in_date_item.update({"Flag": 2, "OutDate": _date})
        #         self._product_init()
        #         r1 = self._save(self.product_client, in_date_item, table_name, fields)
        #         print("***** ", r1)
        #         # （恢复）增加 1 3 4
        #         inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
        #         base_item = {
        #             'TradingType': trading_type, 'InnerCode': inner_code, 'SecuCode': secu_code,
        #             'SecuAbbr': secu_abbr, 'InDate': _date, 'Flag': 1,
        #         }
        #         item1, item3, item4 = copy.copy(base_item), copy.copy(base_item), copy.copy(base_item)
        #         item1.update({'TargetCategory': 1})
        #         item3.update({'TargetCategory': 3})
        #         item4.update({'TargetCategory': 4})
        #         ret = self._batch_save(self.product_client, [item1, item3, item4], table_name, fields)
        #         print("***** ", ret)
        #
        # for item in transfer_1:
        #     # print(item)
        #     secu_code, _date = item
        #     inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
        #     in_date_item = self.get_indate_data(trading_type, table_name, 1, secu_code)
        #     if in_date_item:
        #         # 移出 1
        #         in_date_item.update({"Flag": 2, "OutDate": _date})
        #         r1 = self._save(self.product_client, in_date_item, table_name, fields)
        #         # print(r1)
        #         # 增加 2
        #         item = {'TradingType': trading_type,
        #                 'TargetCategory': 2,
        #                 'InnerCode': inner_code,
        #                 'SecuCode': secu_code,
        #                 'SecuAbbr': secu_abbr,
        #                 'InDate': _date,
        #                 'Flag': 1,
        #
        #                 }
        #         # print("****************", item)
        #         self._product_init()
        #         r2 = self._save(self.product_client, item, table_name, fields)
        #         # print(r2)
        #
        # for item in transfer_134:
        #     print(item)
        #     secu_code, _date = item
        #     self._product_init()
        #     # 移除 1 3 4
        #     for trading_category in (1, 3, 4):
        #         in_date_item = self.get_indate_data(trading_type, table_name, trading_category, secu_code)
        #         if in_date_item:
        #             in_date_item.update({"Flag": 2, "OutDate": _date})
        #             r1 = self._save(self.product_client, in_date_item, table_name, fields)
        #             print("##### ", r1)
        #     # 增加 2
        #     inner_code, secu_abbr = self.get_juyuan_codeinfo(secu_code)
        #     item = {'TradingType': trading_type,
        #             'TargetCategory': 2,
        #             'InnerCode': inner_code,
        #             'SecuCode': secu_code,
        #             'SecuAbbr': secu_abbr,
        #             'InDate': _date,
        #             'Flag': 1,
        #             }
        #     r2 = self._save(self.product_client, item, table_name, fields)
        #     print("***** ", r2)
        #
        # for item in removal_2:
        #     print(item)
        #     secu_code, _date = item
        #     self._product_init()
        #     # 移除 2
        #     in_date_item = self.get_indate_data(trading_type, table_name, 2, secu_code)
        #     if in_date_item:
        #         in_date_item.update({"Flag": 2, "OutDate": _date})
        #         r1 = self._save(self.product_client, in_date_item, table_name, fields)
        #         print("****** ", r1)

    def get_indate_data(self, trading_type, table_name, trading_category, secu_code):
        self._product_init()
        sql = '''select * from {} where TradingType = {} and TargetCategory = {} and SecuCode = {} and Flag = 1; 
        '''.format(table_name, trading_type, trading_category, secu_code)
        ret = self.product_client.select_one(sql)
        return ret

    def start(self):
        """直接检查两个数据点之间的差异"""

        # 两个合资格股的变更表
        origin_change_tables = [
            'hkex_lgt_change_of_sse_securities_lists',
            'hkex_lgt_change_of_szse_securities_lists',
        ]

        info = ''
        count = 1
        for table in origin_change_tables:
            ret = self.get_distinct_spider_udpate_time(table)
            dt_list = sorted([r.get("Time") for r in ret])
            # print("{} 至今全部的更新时间列表是{}".format(table, dt_list))
            # 注意: 最后的两次的差异 以及最后一次与第一次之间的差异 按需
            latest_records = self.select_onetime_records(table, dt_list[-1])
            first_records = self.select_onetime_records(table, dt_list[0])
            print(f"{table}: {dt_list[0]} --> {dt_list[-1]} ")

            # 去掉一些无关字段
            for r in latest_records:
                r.pop("id")
                r.pop("Time")
                r.pop("CREATETIMEJZ")
                r.pop("ItemID")
                r.pop("UPDATETIMEJZ")

            for r in first_records:
                r.pop("id")
                r.pop("Time")
                r.pop("CREATETIMEJZ")
                r.pop("ItemID")
                r.pop("UPDATETIMEJZ")

            to_insert = []
            to_delete = []
            for one in latest_records:
                if not one in first_records and not one in records_sh and not one in records_sz:
                    to_insert.append(one)

            for one in first_records:
                if not one in latest_records and not one in records_sh and not one in records_sz:
                    to_delete.append(one)

            info += "{} 与第一相比 应该删除的记录是: {}\n".format(table, len(to_delete))
            info += "{} 与第一次相比, 应该增加的记录是: {}\n".format(table, len(to_insert))

            if count == 1:
                self.process_sh_changes(to_insert)
            else:
                self.process_sz_changes(to_insert)

            # with open("to_delete_{}.txt".format(count), "w") as f:
            #     f.write(pprint.pformat(to_delete))
            # with open("to_insert_{}.txt".format(count), "w") as f:
            #     f.write(pprint.pformat(to_insert))

            count += 1

        # 检查一致性
        dp = DailyUpdate()
        sh1 = dp.sh_short_sell_list()
        sh2 = dp.sh_buy_margin_trading_list()
        sh3 = dp.sh_only_sell_list()
        sh4 = dp.sh_buy_and_sell_list()

        sz1 = dp.sz_short_sell_list()
        sz2 = dp.sz_buy_margin_trading_list()
        sz3 = dp.sz_only_sell_list()
        sz4 = dp.sz_buy_and_sell_list()

        info += "沪股合资格校对的结果是 {}, \n深股合资格校对的结果是 {}\n".format((sh1, sh2, sh3, sh4), (sz1, sz2, sz3, sz4))
        print(info)
        self.ding(info)


def task():
    checker = OriginChecker()
    checker.start()
    checker.refresh_update_time()


def main():
    logger.info("当前时间是{} ".format(datetime.datetime.now()))
    task()
    schedule.every().day.at("17:00").do(task)

    while True:
        logger.info("当前调度系统中的任务列表是{}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(1800)


if __name__ == "__main__":
    main()

    # task()
