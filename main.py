import datetime
import logging
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from hkland_configs import LOCAL
from hkland_shares.web_spider import shares_spider_task
from hkland_shszhktradingday.tradingdayspider import tradingday_task
from hkland_toptrade.eastmoney_top import EastMoneyTop10
from hkland_toptrade.exchange_top10 import ExchangeTop10
from hkland_toptrade.jqka10_top import JqkaTop10

if LOCAL:
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
else:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


executors = {
    'default': ThreadPoolExecutor(20)
}
ap_scheduler = BackgroundScheduler(executors=executors)


def handle(event_name: str):
    if event_name == 'toptrade':
        # EastMoneyTop10(datetime.datetime.today()).start()
        EastMoneyTop10(datetime.datetime.today() - datetime.timedelta(days=3)).start()
    elif event_name == 'toptrade_exchange':
        # ExchangeTop10(datetime.datetime.today()).start()
        ExchangeTop10(datetime.datetime.today() - datetime.timedelta(days=3)).start()
    elif event_name == 'toptrade_jqka10':
        JqkaTop10().start()

    elif event_name == 'tradingday':
        tradingday_task()
    elif event_name == 'shares':
        shares_spider_task()


# # 在每天的17到19点每隔2min执行一次
# ap_scheduler.add_job(func=handle, trigger="cron", hour='17-19', minute='*/2', args=('toptrade', ), name='toptrade', max_instances=1)
# ap_scheduler.add_job(func=handle, trigger='cron', hour='8,12', args=("tradingday",), name="tradingday", max_instances=1)
#
# ap_scheduler.start()
#
#
# while True:
#     time.sleep(10)


if __name__ == '__main__':
    # handle("toptrade")
    # handle("toptrade_exchange")
    # handle("toptrade_jqka10")
    # handle("tradingday")
    handle("shares")
