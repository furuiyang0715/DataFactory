import datetime
import functools
import os
import pickle
import sys
import time
import traceback

import schedule

sys.path.append("./../")

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


def main():
    logger.info("当前时间是{} ".format(datetime.datetime.now()))
    task()
    schedule.every().day.at("17:00").do(task)

    while True:
        logger.info("当前调度系统中的任务列表是{}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(1800)


main()
