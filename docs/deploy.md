# **slurm适配器安装部署文档**


## **1 Slurm适配器安装部署环境要求（需安装go语言、buf）**
### **1.1 安装go语言、配置go相关环境变量**

```bash
# 下载go语言安装包，安装go
cd download/
wget https://golang.google.cn/dl/go1.19.7.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.19.7.linux-amd64.tar.gz

# 在/etc/profile中设置环境变量
export GOROOT=/usr/local/go
export GOPATH=/usr/local/gopath
export PATH=$PATH:/$GOROOT/bin:$GOPATH/bin

# source环境变量
source /etc/profile

# 验证
go version

# 设置代理
go env -w GOPROXY=https://goproxy.cn,direct

# 开启go mod管理
go env -w GO111MODULE=on
```

### **1.2 安装buf**
```bash
GO111MODULE=on
GOBIN=/usr/local/bin
go install github.com/bufbuild/buf/cmd/buf@v1.19.0
```

## **2 如何配置Slurm适配器项目**

### **2.1 Slurm管理节点上拉取Slurm适配器代码**
```bash
cd /root    # 将slurm适配器代码放在root目录下
git clone https://github.com/PKUHPC/scow-slurm-adapter.git  #克隆代码
```
### **2.2 修改Slurm适配器代码中的配置文件**
```bash
# 进入slurm适配器代码目录
cd /root/scow-slurm-adapter
[root@manage01 scow-slurm-adapter]# ls
buf.gen.yaml  config docs go.mod  go.sum  main.go  Makefile  README.md  tests  utils

# 修改config目录下配置文件config.yaml的配置项
vim config/config.yaml
# slurm 数据库配置
mysql:
  host: 127.0.0.1                                         # slurmdbd服务所在服务器的ip
  port: 3306                                              # slurmdbd服务节点上数据库服务的端口
  user: root                                              # 访问slurmdbd节点数据库服务的用户名
  dbname: slurm_acct_db                                   # 指定slurm数据库的库名
  password: 81SLURM@@rabGTjN7                             # 访问slurmdbd节点数据库的密码
  clustername: cluster                                    # 指定slurm集群的名字

# 服务端口设置
service:
  port: 8999                                              # 指定slurm适配器服务启动端口

# slurm 默认Qos设置
slurm:
  defaultqos: normal                                      # 指定slurm默认qos信息

# module profile文件路径
modulepath:
  path: /lustre/software/module/5.2.0/init/profile.sh     # 指定module profile文件路径
```
**注意：如果slurmdbd服务不在slurm管理节点上，在config.yaml配置文件中指定数据库配置后，还需要在slurmdbd服务所在节点为访问数据库服务的用户授权（只读权限select）。**


## **3 编译项目**
### **3.1 生成proto文件**
```bash
# 在scow-slurm-adapter目录下执行下面命令
[root@manage01 scow-slurm-adapter]# make protos

# 执行完上面的命令后会在当前目录下生成gen目录和相关的proto文件
[root@manage01 scow-slurm-adapter]# ls gen/* 
account_grpc.pb.go  account.pb.go  config_grpc.pb.go  config.pb.go  job_grpc.pb.go  job.pb.go  user_grpc.pb.go  user.pb.go
```

### **3.2 编译项目**
```bash
# 在代码根目录下执行make build生成二进制文件(scow-slurm-adapter-amd64)
[root@manage01 scow-slurm-adapter]# make build 
CGO_BUILD=0 GOARCH=amd64 go build -o scow-slurm-adapter-amd64

[root@manage01 scow-slurm-adapter]# ls
buf.gen.yaml  config  docs gen  go.mod  go.sum  main.go  Makefile  README.md  scow-slurm-adapter-amd64  tests  utils
```


## **4 部署Slurm适配器**
### **4.1 直接在Slurm适配器代码根目录部署slurm适配器（生成二进制文件的目录）**
```bash
# 编译完成后在Slurm适配器代码根目录下执行下面命令完成部署
[root@manage01 scow-slurm-adapter]# nohup ./scow-slurm-adapter-amd64 > server.log 2>&1 &
```

### **4.2 在其他目录中部署Slurm适配器**
```bash
# 在slurm管理节点上创建一个部署目录
mkdir /opt/adapter                                                         # 在opt下创建adapter目录用来部署slurm适配器
cp -r /root/scow-slurm-adapter/config  /opt/adapter                        # 将配置文件目录拷贝至部署目录
cp /root/scow-slurm-adapter/scow-slurm-adapter-amd64  /opt/adapter         # 将编译生成的二进制文件拷贝至部署目录
cd /opt/adapter && nohup ./scow-slurm-adapter-amd64 > server.log 2>&1 &    # 在部署目录中直接启动slurm适配器
```

