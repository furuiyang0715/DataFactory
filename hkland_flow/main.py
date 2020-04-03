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
        schedule.run_pending()
        time.sleep(180)


if __name__ == "__main__":
    main()
