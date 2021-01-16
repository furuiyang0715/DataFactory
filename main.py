import datetime
import logging

from hkland_configs import LOCAL
from hkland_toptrade.eastmoney_top import EastMoneyTop10
from hkland_toptrade.exchange_top10 import ExchangeTop10
from hkland_toptrade.jqka10_top import JqkaTop10

if LOCAL:
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
else:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def schedule_task():
    # 设置时间段的目的是 在非更新时间内 不去无谓的请求
    t_day = datetime.datetime.today()
    start_time = datetime.datetime(t_day.year, t_day.month, t_day.day, 17, 10, 0)
    end_time = datetime.datetime(t_day.year, t_day.month, t_day.day, 20, 10, 0)
    if not (t_day >= start_time and t_day <= end_time):
        logger.warning("不在 17:10 到 20:10 的更新时段内")
        return

    day_str = t_day.strftime("%Y-%m-%d")
    logger.info("今天:", day_str)  # 今天的时间字符串 如果当前还未出 "十大成交股"数据 返回空列表
    EastMoneyTop10(day_str).start()


def task():
    ExchangeTop10().start()
    _now = datetime.datetime.now()
    _year, _month, _day = _now.year, _now.month, _now.day
    _start = datetime.datetime(_year, _month, _day, 16, 0, 0)
    _end = datetime.datetime(_year, _month, _day, 19, 0, 0)
    if _now < _start or _now > _end:
        logger.warning("当前时间 {}, 不在正常的更新时间下午 4 点到 7 点之间".format(_now))
        return
    ExchangeTop10().start()


if __name__ == "__main__":
    JqkaTop10().start()


# if __name__ == '__main__':
#     # schedule_task()
#
#     task()
