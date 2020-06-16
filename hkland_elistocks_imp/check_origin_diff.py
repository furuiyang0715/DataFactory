import datetime
import functools
import os
import pickle
import pprint
import sys
import time
import traceback

import schedule

cur_path = os.path.split(os.path.realpath(__file__))[0]
file_path = os.path.abspath(os.path.join(cur_path, ".."))
sys.path.insert(0, file_path)

from hkland_elistocks_imp.base import logger


# from hkland_elistocks import tools
# from hkland_elistocks.process_changes import process_sh_changes, process_sz_changes
# from hkland_elistocks.snail import records_sh, records_sz
# from hkland_elistocks.my_log import logger
# from hkland_elistocks.list_check import list_check
# from hkland_elistocks.single_process import fix
# from hkland_elistocks.configs import LOCAL, FIRST
# from hkland_elistocks.sh_human_gene import SHHumanTools
# from hkland_elistocks.zh_human_gene import ZHHumanTools


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


def task_2():
    """直接检查两个数据点之间的差异"""
    sh = SHHumanTools()
    zh = ZHHumanTools()
    info = ''
    count = 1
    for ins in (sh, zh):
        ret = ins.get_distinct_spider_udpate_time()
        dt_list = sorted([r.get("Time") for r in ret])
        print("{} 至今全部的更新时间列表是{}".format(ins.table_name, dt_list))
        # 注意: 最后的两次的差异 以及最后一次与第一次之间的差异 按需
        latest_records = ins.select_latest_records()
        first_records = ins.select_onetime_records(dt_list[0])
        # first_records = ins.select_onetime_records(dt_list[-2])

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

        info += "{} 与第一相比 应该删除的记录是: {}\n".format(ins.table_name, len(to_delete))
        info += "{} 与第一次相比, 应该增加的记录是: {}\n".format(ins.table_name, len(to_insert))

        if count == 1:
            process_sh_changes(to_insert)
            pass
        else:
            process_sz_changes(to_insert)

        with open("to_delete_{}.txt".format(count), "w") as f:
            f.write(pprint.pformat(to_delete))
        with open("to_insert_{}.txt".format(count), "w") as f:
            f.write(pprint.pformat(to_insert))

        ins.refresh_update_time()
        count += 1

    r1, r2 = list_check()
    info += "沪股合资格校对的结果是 {}, 深股合资格校对的结果是 {}\n".format(r1, r2)
    print(info)
    # tools.ding_msg(info)


def main():
    logger.info("当前时间是{} ".format(datetime.datetime.now()))
    task_2()
    schedule.every().day.at("17:00").do(task_2)

    while True:
        logger.info("当前调度系统中的任务列表是{}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(1800)


if __name__ == "__main__":
    # main()
    task_2()
