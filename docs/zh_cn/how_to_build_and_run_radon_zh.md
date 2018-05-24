`RadonDB` 不依赖任何第三方库，很容易编译部署

--------------------------------------------------------------------------------------------------
[TOC]

# 如何编译并运行radon(单机模式)

@[Radon]


## 要求
1. [Go](http://golang.org) 支持版本1.8 以上。(如果系统没装go，请使用以下命令安装：

**ubuntu系统** : sudo apt install golang
**centos系统** : yum install golang
**OS X系统** : brew install golang

2. 强烈推荐使用64位系统，以下测试均基于64位系统，32位系统未测试过。

##Step1. 从github下载源码
```
$ git clone https://github.com/radondb/radon
```

##Step2. 编译

源码下载之后，会在本地生成一个目录叫`radon`，执行以下命令：
```
$ cd radon
$ make build
```
编译会生成bin目录，可执行文件在bin目录下，执行命令`ls bin.`
```
$ ls bin/

---Response---
$ radon radoncli
```

##Step3. 运行radon
将默认的配置文件conf/radon.default.json拷贝到bin目录下
```
$ cp conf/radon.default.jsosn bin/
```
 
运行radon服务
```
$ bin/radon -c bin/radon.default.json
``` 
如果成功启动，会显示以下信息：
```
    radon:[{Tag:rc-20180126-16-gf448be1 Time:2018/04/04 03:31:39 Git:f448be1
    GoVersion:go1.8.3 Platform:linux amd64}]
    2018/04/04 15:20:17.136839 proxy.go:79:
     ....
     .... 
    2018/04/04 15:20:17.151499 admin.go:54:      [INFO]     http.server.start[:8080]...
```
radon成功启动的时候，会占用三个端口：

`3308`: 外部服务端口，提供给MySQL客户端连接

`8080`: 管理端口, 外部的`RESTFUL`接口

`6060`: 调试端口, 用于golang调试

## Step4. 添加一个backend(mysql server) 到radon
这是一个`radon API`到管理指令, 详尽到管理指令参见文档：[radon admin API](../api.md).
这里我们假设mysql已经安装到本机并且mysql服务已经启动，登入到mysql的用户名和密码都是`root`，示例如下（具体的用户名跟密码请根据自己的实际情况调整）


`user`: 登入到mysql的用户名
`password`: 登入到mysql的密码

```
$ curl -i -H 'Content-Type: application/json' -X POST -d \
> '{"name": "backend1", "address": "127.0.0.1:3306", "user":\
>  "root", "password": "root", "max-connections":1024}' \
> http://127.0.0.1:8080/v1/radon/backend
```
`Response: `
```
HTTP/1.1 200 OK
Date: Mon, 09 Apr 2018 03:23:02 GMT
Content-Length: 0
Content-Type: text/plain; charset=utf-8
```
## Step5. 使用mysql客户端连接到radon
Radon支持mysql连接协议，连接指令：mysql -uroot   -proot. -h127.0.0.1 -P3308，其中

`-uroot`: 使用账号`root`通过radon登入到mysql-server

`-proot`: 登入账号对应的密码`root`

```
$ mysql -uroot -h127.0.0.1 -P3308

`root`:

```

如果连接成功，则显示如下：

```
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 1
Server version: 5.7-Radon-1.0

Copyright (c) 2000, 2018, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> 
```

现在你可以从客户端发sql, radon当前所支持的sql集详见：[Radon_SQL_surported](../Radon_SQL_surported.md)
`例如: `

```
mysql> SHOW DATABASES;
+--------------------+
| Database           |
+--------------------+
| information_schema |
| db_gry_test        |
| db_test1           |
| mysql              |
| performance_schema |
| sys                |
+--------------------+
6 rows in set (0.01 sec)
```
