import datetime
import sys
import time
import schedule

sys.path.append("./../")

from hkland_shszhktradingday.down_load_lastest_file import download_lastst_csv_file
from hkland_shszhktradingday.gene_trading_days import CSVLoader
from hkland_shszhktradingday.my_log import logger
from hkland_shszhktradingday.parse_page_update_info import get_lastest_update_dt


def task():
    now = datetime.datetime.now()
    logger.info("Now: {}".format(now))
    lastest_update_dt = get_lastest_update_dt()
    logger.info("Last Update Time: {}".format(lastest_update_dt))
    if not lastest_update_dt >= now - datetime.timedelta(days=2):
        logger.info("No Update in Latest 2 Days, Return.")
        return
    print()

    download_lastst_csv_file()

    for file_path, year in [('2019 Calendar_csv_c.csv', 2019), ('2020 Calendar_csv_c.csv', 2020)]:
        ll = CSVLoader(csv_file_path=file_path, year=year)
        ll.start()


def main():
    task()

    schedule.every().day.at("05:00").do(task)

    while True:
        schedule.run_pending()
        time.sleep(180)


main()


if __name__ == "__main__":
    main()
