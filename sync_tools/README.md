数据库的一致性同步工具 

docker build -t registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_sync_tools:v0.0.1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_sync_tools:v0.0.1

sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_sync_tools:v0.0.1


# local 
sudo docker run -itd -v /Users/furuiyang/gitzip/DataFactory/sync_tools:/sync_tools --name sync_tools registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_sync_tools:v0.0.1 /bin/bash 

# remote 
sudo docker run -itd -v /home/furuiyang/sync_tools:/sync_tools --name sync_tools registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/hk_land_sync_tools:v0.0.1 /bin/bash 
