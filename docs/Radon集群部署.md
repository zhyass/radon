`Radon` 集群部署

--------------------------------------------------------------------------------------------------
[TOC]

# Radon集群部署

这部分是讲如何部署Radon集群，默认你已经熟悉Radon单机模式的启动和部署，如不熟悉，请先参看Radon单机模式启动的部署文档。

##Step1.  环境准备
我们将Radon为部署3个节点（1主两从），2个backend节点(mysql-server)，1个backup节点(mysql-server)，需要6台主机（或者虚拟机），部署架构如下所示：

                            +----------------------------+     
                            |  SQL层（3个节点的radon集群）  |  
                            +----------------------------+     
                            |  存储计算层：2个backend节点   |
                            |  和1个backup节点            |  
                            |----------------------------|  



Radon master节点: 192.168.0.16

Radon slave节点1: 192.168.0.17

Radon slave节点1: 192.168.0.18

backend1节点: 192.168.0.14

backend2节点: 192.168.0.28

backup节点  :   192.168.0.15

默认每个机器上的mysql-server有相同的账号和密码，backend1/backend2/backup三个节点的数据库账户假设都为`root`,密码为`123456`，并且每个数据库都授权了可以通从其它IP发起访问，如未设置，请先通过mysql客户端登入到mysql-server并执行：
```
mysql> GRANT ALL PRIVILEGES ON *.* TO root@"%" IDENTIFIED BY '123456'  WITH GRANT OPTION;
```

## Step2. 启动radon
### 2.1 radon master节点(IP: 192.168.0.16)启动:

进入radon/bin目录，执行：
```
$ ./radon -c radon.default.json > radon.log 2>&1 &
```
其中`radon.log`用于记录radon执行的日志
如果要关闭radon进程，执行`lsof`命令，找到radon对应进程号
命令执行完之后，会在当前目录生成新的`bin`目录
```
$ lsof -i :3308
COMMAND   PID   USER   FD   TYPE   DEVICE SIZE/OFF NODE NAME
radon   35572 ubuntu    7u  IPv6 11618866      0t0  TCP *:3308 (LISTEN)
$ kill 35572
```

### 2.2 radon slave1/slave2节点启动
启动方式同master启动.


### 2.3. 查看radon-meta目录下原数据
3个节点都启动之后，在radon/bin目录下用`ls`命令查看新生成的`bin` 目录中radon-meta目录下的json文件
```
$ ls bin/radon-meta/
backend.json
```


## Step3. 通过curl执行add peer指令，构建radon集群

### 3.1 master节点(IP: 192.168.0.16)add peer操作
add  master自身
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.16:8080"}' http://192.168.0.16:8080/v1/peer/add
```

add slave1节点
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.17:8080"}' http://192.168.0.16:8080/v1/peer/add
```

add slave2节点
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.18:8080"}' http://192.168.0.16:8080/v1/peer/add
```

### 3.2 slave1节点(IP: 192.168.0.17)add peer操作

add master节点
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.16:8080"}' http://192.168.0.17:8080/v1/peer/add
```

add slave1自身
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.17:8080"}' http://192.168.0.17:8080/v1/peer/add
```

add slave2节点
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.18:8080"}' http://192.168.0.17:8080/v1/peer/add
```

### 3.3 slave2节点(IP: 192.168.0.18)add peer操作

add master节点
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.16:8080"}' http://192.168.0.18:8080/v1/peer/add
```

add slave1节点
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.17:8080"}' http://192.168.0.18:8080/v1/peer/add
```

add slave2自身
```
curl -i -H 'Content-Type: application/json' -X POST -d '{"address": "192.168.0.18:8080"}' http://192.168.0.18:8080/v1/peer/add
```

### 3.4 再次查看radon-meta目录下原数据
add peer操作完成之后，在radon/bin目录下用`ls`命令查看新生成的`bin` 目录中radon-meta目录下的json文件,可以看到多了peers.json和version.json，peers.json存储集群的节点信息，version.json记录该节点的版本信息，用于节点之间判断是否同步用的。
```
$ ls bin/radon-meta/
backend.json  peers.json  version.json
```

## Step4 给radon master添加backend和backup节点

切换到master节点(`IP: 192.168.0.16`)，到radon/bin目录下，依次执行以下操作

### 4.1 add backend1节点(IP: 192.168.0.14)

```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"name": "backend2", "address": "192.168.0.14:3306", "user":"root", "password": "123456", "max-connections":1024}' http://192.168.0.16:8080/v1/radon/backend
```

### 4.2 add backend2节点(IP: 192.168.0.28)
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"name": "backend1", "address": "192.168.0.28:3306", "user":"root", "password": "123456", "max-connections":1024}' http://192.168.0.16:8080/v1/radon/backend
```

### 4.3 add backup节点(IP: 192.168.0.15)
```
$ curl -i -H 'Content-Type: application/json' -X POST -d '{"name": "backupnode", "address": "192.168.0.15:3306", "user":"root", "password": "123456", "max-connections":1024}' http://192.168.0.16:8080/v1/radon/backup
```

到此，集群就搭建完毕了，此时用vim查看radon-meta下的backend.json文件，可以看到节点信息都已经加进来了。
```
$ vim bin/radon-meta/backend.json 
```
```
{
        "backup": {
                "name": "backupnode",
                "address": "192.168.0.15:3306",
                "user": "root",
                "password": "123456",
                "database": "",
                "charset": "utf8",
                "max-connections": 1024
        },
        "backends": [
                {
                        "name": "backend2",
                        "address": "192.168.0.14:3306",
                        "user": "root",
                        "password": "123456",
                        "database": "",
                        "charset": "utf8",
                        "max-connections": 1024
                },
                {
                        "name": "backend1",
                        "address": "192.168.0.28:3306",
                        "user": "root",
                        "password": "123456",
                        "database": "",
                        "charset": "utf8",
                        "max-connections": 1024
                }
        ]
}
```
再切换到slave1或者slave2节点下都radon/bin目录，用vim查看radon-meta下的backend.json文件，可以看到，虽然slave1和slave2节点没有执行添加backend或backup操作，但是数据已经跟master节点同步了：
```
$ vim bin/radon-meta/backend.json 
```
```
{
        "backup": {
                "name": "backupnode",
                "address": "192.168.0.15:3306",
                "user": "root",
                "password": "123456",
                "database": "",
                "charset": "utf8",
                "max-connections": 1024
        },
        "backends": [
                {
                        "name": "backend2",
                        "address": "192.168.0.14:3306",
                        "user": "root",
                        "password": "123456",
                        "database": "",
                        "charset": "utf8",
                        "max-connections": 1024
                },
                {
                        "name": "backend1",
                        "address": "192.168.0.28:3306",
                        "user": "root",
                        "password": "123456",
                        "database": "",
                        "charset": "utf8",
                        "max-connections": 1024
                }
        ]
}
```

## Step5 通过mysql客户端连接到master

```
$ mysql -h192.168.0.16 -uroot -p123456 -P3308

mysql: [Warning] Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 1038
Server version: 5.7-Radon-1.0 XeLabs TokuDB build 20180118.100653.39b1969

Copyright (c) 2009-2017 Percona LLC and/or its affiliates
Copyright (c) 2000, 2017, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> 
```

下发一条 sql ：
```
mysql> show databases;
+---------------------------+
| Database                  |
+---------------------------+
| information_schema        |
| mysql                     |
| performance_schema        |
| sbtest                    |
| sys                       |
+---------------------------+
5 rows in set (0.13 sec)

mysql>
```

