from urllib.request import urlretrieve


def download_lastst_csv_file():
    # load_2019_file_path = 'https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Reference-Materials/Trading-Hour,-Trading-and-Settlement-Calendar/2019-Calendar_csv_c.csv?la=zh-HK'
    # load_2020_file_path = 'https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Reference-Materials/Trading-Hour,-Trading-and-Settlement-Calendar/2020-Calendar_csv_c.csv?la=zh-HK'

    load_2021_file_path = 'https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Reference-Materials/Trading-Hour,-Trading-and-Settlement-Calendar/2021-Calendar_csv_c.csv?la=zh-HK'
    try:
        # urlretrieve(load_2019_file_path, '2019 Calendar_csv_c.csv')
        # urlretrieve(load_2020_file_path, '2020 Calendar_csv_c.csv')

        urlretrieve(load_2020_file_path, '2021 Calendar_csv_c.csv')
    except:
        print("Update csv file fail.")
    else:
        print("Success.")
