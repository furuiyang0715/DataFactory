import json
import sys
import time
import hmac
import hashlib
import base64
import urllib.parse
import requests as re

from hkland_flow.configs import SECRET, TOKEN


def get_url():
    timestamp = str(round(time.time() * 1000))
    secret_enc = SECRET.encode('utf-8')
    string_to_sign = '{}\n{}'.format(timestamp, SECRET)
    string_to_sign_enc = string_to_sign.encode('utf-8')
    hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    url = 'https://oapi.dingtalk.com/robot/send?access_token={}&timestamp={}&sign={}'.format(TOKEN, timestamp, sign)
    return url


def ding_msg(msg):
    url = get_url()
    header = {
        "Content-Type": "application/json",
        "Charset": "UTF-8"
    }

    message = {
        "msgtype": "text",
        "text": {
            "content": "{}@15626046299".format(msg)
        },
        "at": {
            "atMobiles": [
                "15626046299",
            ],
            "isAtAll": False
        }
    }

    message_json = json.dumps(message)
    try:
        resp = re.post(url=url, data=message_json, headers=header)
    except:
        print("本次钉邮发送失败, 待发送消息的内容是: {}".format(msg))
    else:
        if resp.status_code == 200:
            print("钉钉发送成功: {}".format(msg))
        else:
            print("钉钉消息发送失败 ")

