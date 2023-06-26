# slurm adapter for SCOW

## Build

Requires [Buf]([Buf](https://buf.build/docs/installation/)).

```bash
# Generate code from latest scow-slurm-adapter
make protos

# Build
make build

```

## 1 环境准备
### 1.1 安装golang、环境配置
```bash
# 下载golang安装包、安装golang
cd download/
wget https://golang.google.cn/dl/go1.17.3.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.17.3.linux-amd64.tar.gz

# 设置环境变量, 将下面三行加入到/etc/profile文件最下方
export GOROOT=/usr/local/go
export GOPATH=/usr/local/gopath
export PATH=$PATH:/$GOROOT/bin:$GOPATH/bin

# 加载环境变量
source /etc/profile

# 设置代理
go env -w GOPROXY=https://goproxy.cn,direct

# 开启go mod管理
go env -w GO111MODULE=on
```
### 1.2 安装buf
```bash
GO111MODULE=on
GOBIN=/usr/local/bin
go install github.com/bufbuild/buf/cmd/buf@v1.19.0
```
## 2 拉取Slurm适配器项目代码（在slurm管理节点拉取代码）
```bash
cd /root    # 将slurm适配器代码放在root目录下
git clone https://github.com/PKUHPC/scow-slurm-adapter.git  #克隆代码
```
## 3 修改slurm适配器配置文件
```bash
# 进入slurm适配器代码目录
cd /root/scow-slurm-adapter
[root@manage01 scow-slurm-adapter]# ls
buf.gen.yaml  config  gen  go.mod  go.sum  main.go  Makefile  README.md  tests  utils

# 修改config目录下的配置文件config.yaml
vim config/config.yaml
# slurm 数据库配置
mysql:
  host: 127.0.0.1                                      # slurmdbd服务所在服务器的ip
  port: 3306                                            # slurmdbd服务节点上数据库服务的端口
  user: root                                             # 访问slurmdbd节点数据库服务的用户名
  dbname: slurm_acct_db                     # 指定slurm数据库的库名
  password: 81SLURM@@rabGTjN7   # 访问slurmdbd节点数据库的密码
  clustername: cluster                            # 指定slurm集群的名字

# 服务端口设置
service:
  port: 8999                                             # 指定slurm适配器服务启动端口

# slurm 默认Qos设置
slurm:
  defaultqos: normal                              # 指定slurm默认qos信息

```
**注意：如果slurmdbd服务不在slurm管理节点上，在config.yaml配置文件中指定数据库配置后，还需要在slurmdbd服务所在节点上为访问数据库服务的用户授权。**

## 3 编译slurm适配器
### 3.1 生成proto文件
```bash
# 在scow-slurm-adapter目录下执行下面命令
[root@manage01 scow-slurm-adapter]# make protos
```
### 3.2 编译生成二进制文件
```bash
在scow-slum-adapter 目录下执行编译命令
[root@manage01 scow-slurm-adapter]# make build   
```
**注意：执行完编译命令后会在当前目录生成名为scow-slurm-adapter的二进制文件。**

## 4 部署slurm适配器
### 4.1 直接在代码目录下部署slurm适配器（生成二进制文件的目录） 
```bash
[root@manage01 scow-slurm-adapter]# nohup ./scow-slurm-adapter > server.log 2>&1 &
```
### 4.2 在其他目录部署slurm适配器
```bash
# 在部署机上创建一个部署目录
mkdir /opt/adapter   # 在opt下创建adapter目录用来部署slurm适配器
cp -r /root/scow-slurm-adapter/config  /opt/adapter #将配置文件拷贝至部署目录
cp /root/scow-slurm-adapter/scow-slurm-adapter  /opt/adapter  # 将编译生成的二进制文件拷贝至部署目录
cd /opt/adapter && nohup ./scow-slurm-adapter > server.log 2>&1 &  # 在部署目录中直接启动slurm适配器
```
