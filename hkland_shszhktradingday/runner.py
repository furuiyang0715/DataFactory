import datetime
import logging
import sys
import time
import schedule

sys.path.append("./../")

from hkland_shszhktradingday.down_load_lastest_file import download_lastst_csv_file
from hkland_shszhktradingday.gene_trading_days import CSVLoader
from hkland_shszhktradingday.parse_page_update_info import get_lastest_update_dt

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def task():
    now = datetime.datetime.now()
    logger.info("Now: {}".format(now))
    lastest_update_dt = get_lastest_update_dt()
    logger.info("Last Update Time: {}".format(lastest_update_dt))
    if not lastest_update_dt >= now - datetime.timedelta(days=2):
        logger.info("No Update in Latest 2 Days, Return.")
        return

    logger.info("开始下载更新后的文件")
    download_lastst_csv_file()
    logger.info("下载完毕")

    for file_path, year in [('2019 Calendar_csv_c.csv', 2019), ('2020 Calendar_csv_c.csv', 2020)]:
        logger.info("开始刷新 {} 年的数据".format(year))
        ll = CSVLoader(csv_file_path=file_path, year=year)
        ll.start()


def main():
    task()

    schedule.every().day.at("08:00").do(task)
    schedule.every().day.at("12:00").do(task)

    while True:
        logger.info("当前调度系统中的任务列表是{}".format(schedule.jobs))
        schedule.run_pending()
        time.sleep(180)


main()


if __name__ == "__main__":
    main()


'''delopy
docker build -t registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_trading_days:v0.0.1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_trading_days:v0.0.1
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_trading_days:v0.0.1

sudo docker run --log-opt max-size=10m --log-opt max-file=3 \
-itd --name trade_days --env LOCAL=0 \
registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_trading_days:v0.0.1
'''
