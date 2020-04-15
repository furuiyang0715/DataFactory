import datetime
import re

import requests as req
from lxml import html


def get_lastest_update_dt():
    # 解析页面更新时间 确定是否需要更新 csv 文件
    page_url = 'https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Reference-Materials/Trading-Hour,-Trading-and-Settlement-Calendar?sc_lang=zh-HK'
    body = req.get(page_url).text
    doc = html.fromstring(body)
    '''
    <p class="loadMore__timetag" data-last-updated-display="更新日期 2020年1月28日">更新日期 2020年1月28日</p>
    '''
    update_dt_str = doc.xpath("//p[@class='loadMore__timetag']")[0].text_content()
    # print(update_dt_str)
    dt_str = re.findall(r"更新日期 (\d{4}年\d{1,2}月\d{1,2}日)", update_dt_str)[0]
    # print(dt_str)
    update_dt = datetime.datetime.strptime(dt_str, "%Y年%m月%d日")
    # print(update_dt)
    return update_dt
