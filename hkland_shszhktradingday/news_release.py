# coding=utf8
import datetime
import requests as req
from lxml import html


class CalendarNews(object):
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

    key_words = [
        '国庆节',
        '八号台风信号(天鸽)',
        '圣诞节',
        '元旦',
        '春节',
        '耶稣受难节',
        '复活节',
        '清明节',
        '劳动节',
    ]

    def __init__(self):
        self.url = 'https://sc.hkex.com.hk/TuniS/www.hkex.com.hk/News/News-Release?sc_lang=zh-HK&Year=ALL&NewsCategory=&currentCount={}'.format(2000)
        self.table_name = 'calendar_news'
        self.fields = ['PubDate', 'NewsTag', 'NewsUrl', 'NewsTitle']

    def _create_table(self):
        sql = '''
        CREATE TABLE `{}` (
          `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
          `PubDate` datetime NOT NULL COMMENT '资讯时间',
          `NewsTag` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '资讯分类标签',
          `NewsUrl` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '资讯链接',
          
          
          
          
          `MoneyIn` decimal(20,4) NOT NULL COMMENT '当日资金流入(百万）',
          `MoneyBalance` decimal(20,4) NOT NULL COMMENT '当日余额（百万）',
          `MoneyInHistoryTotal` decimal(20,4) NOT NULL COMMENT '历史资金累计流入(其实是净买额累计流入)(百万元）',
          `NetBuyAmount` decimal(20,4) NOT NULL COMMENT '当日成交净买额(百万元）',
          `BuyAmount` decimal(20,4) NOT NULL COMMENT '买入成交额(百万元）',
          `SellAmount` decimal(20,4) NOT NULL COMMENT '卖出成交额(百万元）',
          `MarketTypeCode` int(11) NOT NULL COMMENT '市场类型代码',
          `MarketType` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '市场类型',
          `CREATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP,
          `UPDATETIMEJZ` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (`id`),
          UNIQUE KEY `un` (`Date`,`MarketTypeCode`)
        ) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='交易所计算陆股通资金流向汇总(港股通币种为港元，陆股通币种为人民币)'
        '''

        pass

    def get_items(self):
        page = req.get(self.url).text
        doc = html.fromstring(page)
        news = doc.xpath("//div[@class='news-releases']/div[@class='news-releases__section']")
        for one in news:
            item = dict()
            _date = one.xpath("./div[@class='news-releases__section--date']/div[@class='news-releases__section--date-day']")[0].text_content()
            _month = one.xpath("./div[@class='news-releases__section--date']/div[@class='news-releases__section--date-month']")[0].text_content()
            _year = one.xpath("./div[@class='news-releases__section--date']/div[@class='news-releases__section--date-year']")[0].text_content()

            _month = self.month_map.get(_month)
            _date = int(_date)
            _year = int(_year)
            news_dt = datetime.datetime(_year, _month, _date)
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
            item['PubDate'] = news_dt
            item['NewsTag'] = news_tag
            item['NewsUrl'] = news_url
            item['NewsTitle'] = news_title


if __name__ == "__main__":
    CalendarNews().get_items()

'''
为陆港通中的非交易日增加说明信息 

在制作陆港通交易日历的过程中, 发现聚源的表会部分给出当天为非交易日的原因。 
涉及到 3 个交易所: 72-香港联交所，83-上海证券交易所，90-深圳证券交易所 
共有 4 个方向: 1-沪股通，2-港股通（沪），3-深股通，4-港股通（深） 

查看每个方向不交易的时间以及原因： 
select EndDate, Reason from qt_shszhsctradingday where Reason is not NULL and TradingType = 1; 

可以大致知道这些备注是针对香港的一些法定节假日以及特殊台风天气。 

如果我们需要制作出含有这些信息的 sql 表, 就可以从这两方面入手。 

首先, 需要了解香港的法定家假日有哪些: 
参考:  https://www.goobnn.cn/tags/1395.html 

1. 元旦 1.1
2. 农历的大年初一到大年初三, 公历时间每年不一致。 
3. 清明节, 根据 https://baike.baidu.com/item/%E6%B8%85%E6%98%8E%E8%8A%82%E4%B9%A0%E4%BF%97/4155712 清明节一般在每年的公历 4.5 前后
4. 耶稣受难节, 根据 https://baike.baidu.com/item/%E8%80%B6%E7%A8%A3%E5%8F%97%E9%9A%BE%E6%97%A5 为复活节的前一个星期五, 公历时间每年不一致
5. 耶稣受难节翌日
6. 复活节 根据: https://www.diyifanwen.com/fanwen/fuhuojie/3476303.html 
复活节是西方传统的节日, 公元 325 年尼西亚宗教会议规定, 每年过春分月圆后的第一个星期天为复活节。
其日期是不固定的，通常是要查看日历才能知道。 
一般是在旧历的 3.22 至 4.23 之间, 确切日子要根据春分或其后出现的满月决定。 
7. 劳动节 5.1 
8. 佛诞 根据: https://www.baike.com/wiki/%E4%BD%9B%E8%AF%9E?view_id=ua4dagabnls00, 又称沐佛节，为每年的农历四月初八，是佛祖释迦牟尼的诞辰。  
9. 端午节: 根据: https://baike.baidu.com/item/%E7%AB%AF%E5%8D%88%E8%8A%82/1054 时间在农历 五月初五
10. 香港特别行政区成立纪念日 7.1 
11. 中秋节 农历八月十五
12. 国庆节 10.1 
13. 重阳节 农历九月初九
14. 圣诞节 公历 12.25


交易日历的最早开始时间: 
将沪港通的交易日历扩展到 其开始时间： 2014年11月17日，将深港通日历扩展到其开始时间： 2016年12月5日
 
发现其实可以根据新浪的节日安排来: 
# 2014 年

# 2015 年

# 2016 年 


# 2017 年 
http://finance.sina.com.cn/stock/hkstock/hkstocknews/2017-01-04/doc-ifxzczff3804335.shtml 
# 2018 年
http://finance.sina.com.cn/stock/hkstock/hkstocknews/2017-12-30/doc-ifyqcwaq6021399.shtml 
# 2019 年 
https://new.qq.com/omn/20181230/20181230A0H4W0.html 
# 2020 年 
https://finance.sina.com.cn/roll/2020-01-01/doc-iihnzahk1315924.shtml 

'''
