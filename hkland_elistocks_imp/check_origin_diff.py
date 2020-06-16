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
from hkland_elistocks_imp.configs import LOCAL
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
        self.is_local = LOCAL

    def get_distinct_spider_udpate_time(self, table_name):
        self._test_init()
        self._spider_init()
        sql = '''select distinct(Time) from {};'''.format(table_name)
        if self.is_local:
            ret = self.test_client.select_all(sql)
        else:
            ret = self.spider_client.select_all(sql)
        return ret

    def select_onetime_records(self, table_name, onetime: datetime.datetime):
        """获取某一个时间点的记录"""
        self._test_init()
        self._spider_init()
        sql = '''select * from {} where Time = '{}'; '''.format(table_name, onetime)
        if self.is_local:
            ret = self.test_client.select_all(sql)
        else:
            ret = self.spider_client.select_all(sql)
        return ret

    def process_sz_changes(self, changes):
        change_removal = 'Removal'
        change_addition = 'Addition'
        change_transfer = 'Transfer to List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only)'
        change_recover = 'Addition (from List of Special SZSE Securities/Special China Connect Securities (stocks eligible for sell only))'
        addition_sentence = 'This stock will also be added to the List of Eligible SZSE Securities for Margin Trading and the List of Eligible SZSE Securities for Short Selling'
        recover_sentence = 'This stock will also be added to the List of Eligible SZSE Securities for Margin Trading and the List of Eligible SZSE Securities for Short Selling as it is also included in SZSE stock list for margin trading and shortselling.'
        remove_sentence = 'This stock will also be removed from the List of Eligible SZSE Securities for Margin Trading and the List of Eligible SZSE Securities for Short Selling.'

        self.process("sz", changes, change_removal, change_addition, change_transfer, change_recover,
                     addition_sentence, recover_sentence, remove_sentence)

    def process_sh_changes(self, changes):
        change_removal = 'Removal'
        change_addition = 'Addition'
        change_transfer = 'Transfer to List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only)'
        change_recover = 'Addition (from List of Special SSE Securities/Special China Connect Securities (stocks eligible for sell only))'

        addition_sentence = 'This stock will also be added to the List of Eligible SSE Securities for Margin Trading and the List of Eligible SSE Securities for Short Selling'
        recover_sentence = 'This stock will also be added to the List of Eligible SSE Securities for Margin Trading and the List of Eligible SSE Securities for Short Selling as it is also included in SSE stock list for margin trading and shortselling.'
        remove_sentence = 'This stock will also be removed from the List of Eligible SSE Securities for Margin Trading and the List of Eligible SSE Securities for Short Selling.'

        self.process("sh", changes, change_removal, change_addition, change_transfer, change_recover,
                     addition_sentence, recover_sentence, remove_sentence)

    def process(self, flag, changes, change_removal, change_addition, change_transfer, change_recover,
                addition_sentence, recover_sentence, remove_sentence):
        add_134 = []
        add_1 = []

        recover_1 = []
        recover_134 = []

        transfer_1 = []
        transfer_134 = []

        removal_2 = []
        for change in changes:
            _change, _remarks, secu_code = change.get('Ch_ange'), change.get("Remarks"), change.get("SSESCode")
            _effectivedate = change.get("EffectiveDate")
            if _change == change_addition:
                if addition_sentence in _remarks:
                    add_134.append((secu_code, _effectivedate))
                else:
                    add_1.append((secu_code, _effectivedate))
            elif _change == change_recover:
                if recover_sentence in _remarks:
                    recover_134.append((secu_code, _effectivedate))
                else:
                    recover_1.append((secu_code, _effectivedate))
            elif _change == change_transfer:
                if remove_sentence in _remarks:
                    transfer_134.append((secu_code, _effectivedate))
                else:
                    transfer_1.append((secu_code, _effectivedate))
            elif _change == change_removal:
                removal_2.append((secu_code, _effectivedate))

        print("{}_add_1: ".format(flag), add_1)
        print("{}_add_134: ".format(flag), add_134)

        print("{}_recover_1: ".format(flag), recover_1)
        print("{}_recover_134: ".format(flag), recover_134)

        print("{}_remove_1: ".format(flag), transfer_1)
        print("{}_remove_134".format(flag), transfer_134)

        print("{}_removal_2".format(flag), removal_2)

    def start(self):
        """直接检查两个数据点之间的差异"""
        _map = {
            'hkland_hgelistocks': '',   # 沪港通合资格股
            'hkland_sgelistocks': '',   # 深港通合资格股

        }
        origin_map = {
            'hkex_lgt_change_of_sse_securities_lists': '',
            'hkex_lgt_change_of_szse_securities_lists': '',
        }
        info = ''
        count = 1
        for table in origin_map:
            ret = self.get_distinct_spider_udpate_time(table)
            dt_list = sorted([r.get("Time") for r in ret])
            print("{} 至今全部的更新时间列表是{}".format(table, dt_list))
            # 注意: 最后的两次的差异 以及最后一次与第一次之间的差异 按需
            latest_records = self.select_onetime_records(table, dt_list[-1])
            first_records = self.select_onetime_records(table, dt_list[0])

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

            with open("to_delete_{}.txt".format(count), "w") as f:
                f.write(pprint.pformat(to_delete))
            with open("to_insert_{}.txt".format(count), "w") as f:
                f.write(pprint.pformat(to_insert))
            count += 1

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
    OriginChecker().start()


# task()

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

'''
docker build -t registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_scelistocks:v0.0.1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_scelistocks:v0.0.1
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_scelistocks:v0.0.1

sudo docker run --log-opt max-size=10m --log-opt max-file=3 -itd --name scelistocks_imp \
--env LOCAL=0 \
registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_scelistocks:v0.0.1

'''