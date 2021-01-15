import datetime
import logging

from hkland_configs import LOCAL
from hkland_toptrade.eastmoney_top import EastMoneyTop10

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


if __name__ == '__main__':
    schedule_task()
