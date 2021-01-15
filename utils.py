import base64
import hashlib
import hmac
import json
import logging
import time
import urllib.parse
import requests

from hkland_configs import SECRET, TOKEN, USER_PHONE

logger = logging.getLogger()


def ding_msg(msg: str):
    """发送钉钉预警消息"""
    def get_url():
        timestamp = str(round(time.time() * 1000))
        secret_enc = SECRET.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, SECRET)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        url = 'https://oapi.dingtalk.com/robot/send?access_token={}&timestamp={}&sign={}'.format(
            TOKEN, timestamp, sign)
        return url

    url = get_url()
    header = {
        "Content-Type": "application/json",
        "Charset": "UTF-8"
    }
    message = {
        "msgtype": "text",
        "text": {
            "content": f"{msg}@{USER_PHONE}"
        },
        "at": {
            "atMobiles": [
                USER_PHONE,
            ],
            "isAtAll": False
        }
    }
    message_json = json.dumps(message)
    resp = requests.post(url=url, data=message_json, headers=header)
    if resp.status_code == 200:
        logger.info("钉钉发送消息成功: {}".format(msg))
    else:
        logger.warning("钉钉消息发送失败")
