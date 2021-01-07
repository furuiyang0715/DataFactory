import datetime
import logging
import sys
import time
import schedule
import traceback
import re
import requests as req
from lxml import html
from urllib.request import urlretrieve

sys.path.append("./../")
from hkland_shszhktradingday.gene_trading_days import CSVLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def download_lastst_csv_file():
    # load_2019_file_path = 'https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Reference-Materials/Trading-Hour,-Trading-and-Settlement-Calendar/2019-Calendar_csv_c.csv?la=zh-HK'
    # load_2020_file_path = 'https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Reference-Materials/Trading-Hour,-Trading-and-Settlement-Calendar/2020-Calendar_csv_c.csv?la=zh-HK'
    load_2021_file_path = 'https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Reference-Materials/Trading-Hour,-Trading-and-Settlement-Calendar/2021-Calendar_csv_c.csv?la=zh-HK'
    try:
        # urlretrieve(load_2019_file_path, '2019 Calendar_csv_c.csv')
        # urlretrieve(load_2020_file_path, '2020 Calendar_csv_c.csv')
        urlretrieve(load_2021_file_path, '2021 Calendar_csv_c.csv')
    except:
        logger.debug(f"Update csv file fail. {traceback.format_exc()}")
    else:
        logger.info("Download success.")


def get_lastest_update_dt():
    # 解析页面更新时间 确定是否需要更新 csv 文件
    page_url = 'https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Reference-Materials/Trading-Hour,-Trading-and-Settlement-Calendar?sc_lang=zh-HK'
    body = req.get(page_url).text
    doc = html.fromstring(body)
    '''
    <p class="loadMore__timetag" data-last-updated-display="更新日期 2020年1月28日">更新日期 2020年1月28日</p>
    '''
    update_dt_str = doc.xpath("//p[@class='loadMore__timetag']")[0].text_content()
    dt_str = re.findall(r"更新日期 (\d{4}年\d{1,2}月\d{1,2}日)", update_dt_str)[0]
    update_dt = datetime.datetime.strptime(dt_str, "%Y年%m月%d日")
    logger.info(f"上一次的更新时间是 {update_dt}")
    return update_dt


def update_calendar():
    logger.info("开始下载更新后的文件")
    download_lastst_csv_file()
    logger.info("下载完毕")

    for file_path, year in [
                            # ('2019 Calendar_csv_c.csv', 2019),
                            # ('2020 Calendar_csv_c.csv', 2020),
                            ('2021 Calendar_csv_c.csv', 2021),

    ]:
        logger.info("开始刷新 {} 年的数据".format(year))
        ll = CSVLoader(csv_file_path=file_path, year=year)
        ll.start()


def task():
    _now = datetime.datetime.now()
    logger.info(f"Now: {_now}")
    try:
        lastest_update_dt = get_lastest_update_dt()
    except:
        lastest_update_dt = None
    logger.info("Last Update Time: {}".format(lastest_update_dt))
    if lastest_update_dt and (not lastest_update_dt >= _now - datetime.timedelta(days=2)):
        logger.info("No Update in Latest 2 Days, Return.")
        return

    update_calendar()


def main():
    update_calendar()
    task()

    schedule.every().day.at("08:00").do(task)
    schedule.every().day.at("12:00").do(task)

    while True:
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

# 手动更新一次扩充程序 
docker exec -it trade_days /bin/bash 
python extend_calendar.py 
'''
