FROM registry.cn-shenzhen.aliyuncs.com/jzdev/tinibase:1.0.0

ENV TZ=Asia/Shanghai

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN mkdir /DataFactory

WORKDIR /DataFactory

ADD . /DataFactory

WORKDIR /DataFactory

RUN pip install -r requirements.txt -i https://pypi.douban.com/simple

ENTRYPOINT ["python", "main.py"]
