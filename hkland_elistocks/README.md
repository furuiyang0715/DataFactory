docker build -t registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_scelistocks:v0.0.1 .
docker push registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_scelistocks:v0.0.1

sudo docker pull registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_scelistocks:v0.0.1
sudo docker run -itd --name scelistocks --env LOCAL=0 registry.cn-shenzhen.aliyuncs.com/jzdev/dcfactory/kland_scelistocks:v0.0.1
