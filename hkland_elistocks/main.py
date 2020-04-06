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
    for ins in (sh, zh):
        print(">>>"*50)
        ret = ins.get_distinct_spider_udpate_time()
        dt_list = sorted([r.get("Time") for r in ret])
        latest_records = ins.select_latest_records()
        last_but_one_records = ins.select_onetime_records(dt_list[-2])

        # 去掉一些无关字段
        for r in latest_records:
            r.pop("id")
            r.pop("Time")
            r.pop("CREATETIMEJZ")
            r.pop("ItemID")
            r.pop("UPDATETIMEJZ")

        for r in last_but_one_records:
            r.pop("id")
            r.pop("Time")
            r.pop("CREATETIMEJZ")
            r.pop("ItemID")
            r.pop("UPDATETIMEJZ")

        to_insert = []
        to_delete = []
        for one in latest_records:
            if not one in last_but_one_records:
                to_insert.append(one)

        for one in last_but_one_records:
            if not one in latest_records:
                to_delete.append(one)

        print(pprint.pformat(to_delete))
        print(pprint.pformat(to_insert))

        for one in to_insert:
            secu_code = one.get("SSESCode")
            change = one.get("Ch_ange")
            remarks = one.get("Remarks")
            effective_date = one.get("EffectiveDate")
            update_dt = one.get("UPDATETIMEJZ")
            if change == ins.stats_transfer:
                if ins.sentense3 in remarks:
                    logger.info("结束 1 3 4, 生成 2")
                else:
                    logger.info("结束 1, 生成 2")
                    records = ins.show_code_target_records(secu_code)
                    in_record = None
                    for r in records:
                        if r.get("Flag") == 1 and r.get('TargetCategory') == 1:
                            in_record = copy.deepcopy(r)
                    if in_record:
                        in_record.update({'OutDate': effective_date, 'Flag': 2})
                        in_record.pop("CREATETIMEJZ")
                        in_record.pop("UPDATETIMEJZ")
                        logger.info(in_record)
                        ins.update(in_record)

                    record_new = copy.deepcopy(records[0])
                    record_new.update({"TargetCategory": 2, 'InDate': effective_date, "OutDate": None, 'Flag': 1, 'CMFTime': update_dt})
                    record_new.pop("CREATETIMEJZ")
                    record_new.pop("UPDATETIMEJZ")
                    record_new.pop("ID")
                    ins.update(record_new)
            elif change == 'Buy orders resumed':
                print("触发持股比例暂停买入")
                secu_code = one.get("SSESCode")
                change = one.get("Ch_ange")
                remarks = one.get("Remarks")
                effective_date = one.get("EffectiveDate")
                update_dt = one.get("UPDATETIMEJZ")

                records = ins.show_code_target_records(secu_code)
                record_new = copy.deepcopy(records[0])

                record_new.update({"TargetCategory": 5, 'InDate': effective_date, "OutDate": None, 'Flag': 1, 'CMFTime': update_dt})
                record_new.pop("CREATETIMEJZ")
                record_new.pop("UPDATETIMEJZ")
                record_new.pop("ID")
                # print(record_new)
                # print(ins.table_name)
                ins.update(record_new)

        # 更新最新插入时间
        ins.refresh_update_time()

    print(">>>" * 50)
    list_check()


# task_2()


def main():
    logger.info("当前时间是{} ".format(datetime.datetime.now()))
    task_2()
    schedule.every().day.at("17:00").do(task_2)

    while True:
        logger.info("当前调度系统中的任务列表是{}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(1800)


main()
