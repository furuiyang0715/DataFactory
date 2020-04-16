# python 3.8
import json
import time
import hmac
import hashlib
import base64
import urllib.parse
import requests as re


def get_url():
    timestamp = str(round(time.time() * 1000))
    secret = 'SECb57abcb8d6926df6e4e21e11d932d1105f667ee399de69adacd0ae53aa7b01dd'
    token = 'a604da37f257f4557a748da9a40bdfb2e29fcafad4df34ff94a7dd05c840d506'
    secret_enc = secret.encode('utf-8')
    string_to_sign = '{}\n{}'.format(timestamp, secret)
    string_to_sign_enc = string_to_sign.encode('utf-8')
    hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
    sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
    url = 'https://oapi.dingtalk.com/robot/send?access_token={}&timestamp={}&sign={}'.format(token, timestamp, sign)
    return url


def ding_push_message(url, msg: str):
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
    resp = re.post(url=url, data=message_json, headers=header)
    if resp.status_code == 200:
        print("发送成功 ")
    else:
        print("消息发送失败 ")


if __name__ == "__main__":
    url = get_url()
    msg = "error!!!"
    ding_push_message(url, msg)
