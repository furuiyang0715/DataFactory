部署: 
docker build -t registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_trading_days:v0.0.1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_trading_days:v0.0.1
sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_trading_days:v0.0.1
sudo docker run -itd --name trade_days --env LOCAL=0 registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_trading_days:v0.0.1

逻辑: 
