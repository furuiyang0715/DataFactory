import copy
import datetime
import functools
import os
import pickle
import pprint
import sys
import time
import traceback

import schedule

sys.path.append("./../")

from hkland_elistocks import tools
from hkland_elistocks.snail import records_sh, records_sz
from hkland_elistocks.my_log import logger
from hkland_elistocks.list_check import list_check
from hkland_elistocks.single_process import fix
from hkland_elistocks.configs import LOCAL, FIRST
from hkland_elistocks.sh_human_gene import SHHumanTools
from hkland_elistocks.zh_human_gene import ZHHumanTools


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


def check_ins_diff(ins):
    latest_records = ins.select_latest_records()

    try:
        with open(os.path.join("/hkland_elistocks", ins.change_table_name.upper() + ".pickle"), 'rb') as f:
            content = f.read()
            origin_records = pickle.loads(content)
    except:
        traceback.print_exc()
        origin_records = []

    with open(os.path.join("/hkland_elistocks", ins.change_table_name.upper() + ".pickle"), 'wb+') as f:
        f.write(pickle.dumps(latest_records))

    # 对于 origin_records 以及 latest_records 都去除时间 Time 进行比较
    for r in origin_records:
        r.pop("id")
        r.pop("Time")
        r.pop("CREATETIMEJZ")
        r.pop("ItemID")
        r.pop("UPDATETIMEJZ")

    for r in latest_records:
        r.pop("id")
        r.pop("Time")
        r.pop("CREATETIMEJZ")
        r.pop("ItemID")
        r.pop("UPDATETIMEJZ")

    logger.info(f"Length of origin records: {len(origin_records)}")
    logger.info(f"Length of latest records: {len(latest_records)}")

    to_delete = []
    to_insert = []
    for r in origin_records:
        if not r in latest_records:
            to_delete.append(r)

    for r in latest_records:
        if not r in origin_records:
            to_insert.append(r)

    return to_delete, to_insert


def first_generate():
    sh = SHHumanTools()
    if LOCAL:
        sh.create_target_table()
    sh._process()

    zh = ZHHumanTools()
    if LOCAL:
        zh.create_target_table()
    zh._process()

    fix()

    list_check()


def task():
    if FIRST:
        first_generate()
    else:
        sh = SHHumanTools()
        sh_to_delete, sh_to_insert = check_ins_diff(sh)

        zh = ZHHumanTools()
        zh_to_delete, zh_to_insert = check_ins_diff(zh)

        logger.info(f"sh_to_delete: {len(sh_to_delete)}, sh_to_insert: {len(sh_to_insert)} ")
        logger.info(f"zh_to_delete: {len(zh_to_delete)}, zh_to_insert: {len(zh_to_insert)} ")

        if not sh_to_insert and not sh_to_delete and not zh_to_insert and not zh_to_delete:
            logger.info("无增量")
            list_check()
        else:
            logger.info("开始增量处理")


def task_2():
    """直接检查两个数据点之间的差异"""
    sh = SHHumanTools()
    zh = ZHHumanTools()
    info = ''
    for ins in (sh, zh):
        ret = ins.get_distinct_spider_udpate_time()
        dt_list = sorted([r.get("Time") for r in ret])
        print("{} 至今全部的更新时间列表是{}".format(ins.table_name, dt_list))
        # 注意: 最后的两次的差异 以及最后一次与第一次之间的差异 按需
        latest_records = ins.select_latest_records()
        # first_records = ins.select_onetime_records(dt_list[0])
        first_records = ins.select_onetime_records(dt_list[-2])

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

        # print(pprint.pformat(to_delete))
        # print(pprint.pformat(to_insert))
        info += "{} 与第一相比 应该删除的记录是: {}\n".format(ins.table_name, len(to_delete))
        info += "{} 与第一次相比, 应该增加的记录是: {}\n".format(ins.table_name, len(to_insert))
        ins.refresh_update_time()

    r1, r2 = list_check()
    info += "沪股合资格校对的结果是 {}, 深股合资格校对的结果是 {}\n".format(r1, r2)
    print(info)
    tools.ding_msg(info)


# task_2()


def main():
    logger.info("当前时间是{} ".format(datetime.datetime.now()))
    task_2()
    schedule.every().day.at("17:00").do(task_2)

    while True:
        logger.info("当前调度系统中的任务列表是{}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(1800)


if __name__ == "__main__":
    main()
