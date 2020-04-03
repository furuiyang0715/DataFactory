import time

import schedule

from hkland_flow.hk_flow_process import EMLGTNanBeiXiangZiJin


def spider_task():
    eml = EMLGTNanBeiXiangZiJin()
    eml.start()


def sync_task():
    eml = EMLGTNanBeiXiangZiJin()
    eml.sync()


def main():
    schedule.every(3).seconds.do(spider_task)
    schedule.every(5).seconds.do(sync_task)

    while True:
        print("当前调度系统中的任务列表是{}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
