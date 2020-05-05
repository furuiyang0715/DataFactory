# coding=utf8
import datetime
import sys
import traceback

import pymongo

import requests as req
from lxml import html


month_map = {
    "一月": 1,
    "二月": 2,
    "三月": 3,
    "四月": 4,
    "五月": 5,
    "六月": 6,
    "七月": 7,
    "八月": 8,
    "九月": 9,
    "十月": 10,
    "十一月": 11,
    "十二月": 12,
}

client = pymongo.MongoClient("127.0.0.1:27017").news.stock_news


def get_items():
    """将网站数据存入 mongo"""
    url = 'https://sc.hkex.com.hk/TuniS/www.hkex.com.hk/News/News-Release?sc_lang=zh-HK&Year=ALL&NewsCategory=&currentCount={}'.format(2000)
    page = req.get(url).text
    # print(page)
    doc = html.fromstring(page)
    news = doc.xpath("//div[@class='news-releases']/div[@class='news-releases__section']")
    # print(news)
    for one in news:
        item = {}
        # print(one)
        # print(one.text_content())
        # 日
        _date = one.xpath("./div[@class='news-releases__section--date']/div[@class='news-releases__section--date-day']")[0].text_content()
        # 月
        _month = one.xpath("./div[@class='news-releases__section--date']/div[@class='news-releases__section--date-month']")[0].text_content()
        # 年
        _year = one.xpath("./div[@class='news-releases__section--date']/div[@class='news-releases__section--date-year']")[0].text_content()

        _month = month_map.get(_month)
        _date = int(_date)
        _year = int(_year)
        # print(_month)
        # print(_date)
        # print(_year)
        news_dt = datetime.datetime(_year, _month, _date)
        # print(news_dt)
        try:
            news_tag = one.xpath(".//span[@class='tag-yellow  tag-yellow-triangle tag-first']")[0].text_content()
        except:
            news_tag = None
        try:
            news_url = one.xpath(".//a[@class='news-releases__section--content-title']/@href")[0]
            news_title = one.xpath(".//a[@class='news-releases__section--content-title']")[0].text_content()
        except:
            news_url = one.xpath(".//a[@class='news-releases__section--content-title ']/@href")[0]
            news_title = one.xpath(".//a[@class='news-releases__section--content-title ']")[0].text_content()
        # print(news_url)
        # print(news_tag)
        # print(news_title)
        # print()
        item['PubDate'] = news_dt
        item['NewsTag'] = news_tag
        item['NewsUrl'] = news_url
        item['NewsTitle'] = news_title
        print(item)
        try:
            client.insert_one(item)
        except:
            # traceback.print_exc()
            print(">>>>>>>> ", item)


def get_untrade_days():
    """获取非周末的非交易日"""

    pass


def get_already_remarks():
    """获取已知的暂停交易备注信息"""

    pass


if __name__ == "__main__":
    # get_items()

    pass
