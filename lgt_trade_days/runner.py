import datetime
import sys
sys.path.append("./../")

from lgt_trade_days.down_load_lastest_file import download_lastst_csv_file
from lgt_trade_days.gene_trading_days import CSVLoader
from lgt_trade_days.my_log import logger
from lgt_trade_days.parse_page_update_info import get_lastest_update_dt


def main():
    # now = datetime.datetime.now()
    # logger.info("Now: {}".format(now))
    # lastest_update_dt = get_lastest_update_dt()
    # logger.info("Last Update Time: {}".format(lastest_update_dt))
    # if not lastest_update_dt >= now - datetime.timedelta(days=2):
    #     logger.info("No Update in Latest 2 Days, Return.")
    #     return

    download_lastst_csv_file()

    for file_path, year in [('2019 Calendar_csv_c.csv', 2019), ('2020 Calendar_csv_c.csv', 2020)]:
        ll = CSVLoader(csv_file_path=file_path, year=year)
        ll.start()


if __name__ == "__main__":
    main()