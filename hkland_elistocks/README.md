docker build -t registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_scelistocks:v0.0.1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_scelistocks:v0.0.1

sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_scelistocks:v0.0.1
sudo docker run -itd --name scelistocks --env LOCAL=0 --env FIRST=1 registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_scelistocks:v0.0.1

sudo docker run -itd --name scelistocks --env LOCAL=0 --env FIRST=0 registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_scelistocks:v0.0.1


日常运维： 
1. 进入 sshd1 的 sync_tools 容器， docker exec -it sync_tools /bin/bash ，运行爬虫至测试库的同步程序： python sync_spider2test.py 
2. 运行 main.py 进行自检
3. main 的效果是检出两个时间点之间的原始记录的差异 
