from urllib.request import urlretrieve

from lgt_trade_days.my_log import logger


def download_lastst_csv_file():
    load_2020_file_path = 'https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Reference-Materials/Trading-Hour,-Trading-and-Settlement-Calendar/2020-Calendar_csv_c.csv?la=zh-HK'
    load_2019_file_path = 'https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Reference-Materials/Trading-Hour,-Trading-and-Settlement-Calendar/2019-Calendar_csv_c.csv?la=zh-HK'

    try:
        urlretrieve(load_2019_file_path, '2019 Calendar_csv_c.csv')
        urlretrieve(load_2020_file_path, '2020 Calendar_csv_c.csv')
    except:
        logger.warning("Update csv file fail.")
    else:
        logger.info("Success.")
