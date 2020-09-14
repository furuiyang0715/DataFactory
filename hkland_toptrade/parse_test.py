# import json
# import pprint
# import sys
#
# import requests
#
# # url = 'https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Statistics/Historical-Daily?sc_lang=zh-HK#select4=1&select5=0&select3=0&select1=16&select2=5'
# url = 'https://www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_20200617c.js?_=1592465188627'
# # url = 'https://www.hkex.com.hk/chi/csm/DailyStat/data_tab_daily_20200501c.js?_=1592465188627'
#
# resp = requests.get(url)
#
# if resp.status_code == 200:
#     body = resp.text
#     # print(body)
#     # print(type(body))
#     datas_str = body.replace("tabData = ", "")
#     try:
#         datas = eval(datas_str)
#     except:
#         sys.exit(0)
#     # datas = json.loads(datas_str)
#     # print(datas)
#     # print(type(datas))
#     # print(pprint.pformat(datas))
#     for direction_data in datas:
#         cur_dt = direction_data.get("date")
#         market = direction_data.get("market")
#         is_trading_day = direction_data.get("tradingDay")
#         content = direction_data.get("content")[1].get("table")
#         print(cur_dt)
#         print(market)
#         print(is_trading_day)
#         print(pprint.pformat(content))
#
#         print()
#         print()
#
# else:
#     print(resp)
