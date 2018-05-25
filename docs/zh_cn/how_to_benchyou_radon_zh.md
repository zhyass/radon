Contents
=================

* [如何使用benchyou进行压测](#如何使用benchyou进行压测)
   * [step1 make build](#step1-make-build)
      * [1.1 源码下载](#11-源码下载) 
      * [1.2 修改源码](#12-修改源码) 
      * [1.3 编译](#13-编译) 
   * [step2 搭建radon集群](#step2-搭建radon集群)
   * [step3  创建database: sbtest](#step3--创建database-sbtest)
   * [step4 使用benchyou建表](#step4-使用benchyou建表)
   * [step5  开始压测](#step5--开始压测)
      * [5.1 关闭分布式事务，关闭审计功能，随机写](#51-关闭分布式事务关闭审计功能随机写)
      * [5.2 关闭分布式事务，关闭审计功能，随机读](#52-关闭分布式事务关闭审计功能随机读) 
      * [5.3 关闭分布式事务，关闭审计功能，随机混合读写](#53-关闭分布式事务关闭审计功能随机混合
 )        
      * [5.4 开启分布式事务，关闭审计功能, 随机混合读写](#54-开启分布式事务关闭审计功能-随机混合读写)
      * [5.5 开启分布式事务，开启审计功能, 随机混合读写](#55-开启分布式事务开启审计功能-随机混合读写)

# 如何使用benchyou进行压测 

## step1 make build
### 1.1 源码下载

```
$ git clone https://github.com/xelabs/benchyou
$ cd benchyou
$ ls
LICENSE   README.md bin       makefile  pkg       src
```

### 1.2 修改源码

`Note:` benchyou用于对mysql和radon进行性能压测，由于radon建表需要指定partion by hash(key)，需要对benchyou代码建表部分进行调整。
调整源码的位置在：src/sysbench/table.go文件的41行位置，将
```
) engine=%s`, i, engine)
```

调整为:

```
) engine=%s partition by hash(id)`, i, engine)
```

### 1.3 编译

```
$ make build
--> go get...
go get github.com/xelabs/go-mysqlstack/driver
--> Building...
go build -v -o bin/benchyou src/bench/benchyou.go
vendor/golang.org/x/crypto/ed25519/internal/edwards25519
vendor/golang.org/x/crypto/curve25519
xcommon
vendor/github.com/spf13/pflag
xworker
vendor/golang.org/x/crypto/ed25519
sysbench
vendor/golang.org/x/crypto/ssh
vendor/github.com/spf13/cobra
xstat
xcmd
```

## step2 搭建radon集群

详细的搭建教程，见[radon_cluster_deployment_zh.md](radon_cluster_deployment_zh.md)

## step3  创建database: sbtest

benchyou在进行压测时，默认使用的数据库是sbtest，因而在radon集群搭建完毕后，通过mysql登入到radon，创建sbtest数据库

```
mysql> CREATE DATABASE SBTEST;
```

## step4 使用benchyou建表

建表指令如下，相关参数说明

`--mysql-host=192.168.0.16` : radon master节点

`--mysql-port` : 由于我们压测的是radon，因此这里的port是radon master节点对应的port

`--mysql-user=root` :  mysql登入账号

`--mysql-password=123456 `: mysql登入密码

`-oltp-tables-count` ： 创建表的数目

`--mysql-table-engine=innod` : 指定引擎为innodb

`--max-request` : 最大请求次数,比如执行insert,就表示写入1000万条数据

```
$ ./benchyou  --mysql-host=192.168.0.16 --mysql-port=3308 --mysql-user=root --mysql-password=123456 --oltp-tables-count=32  --mysql-table-engine=innodb   --max-request=1000000 prepare
```

```
2018/05/23 14:19:29 create table benchyou0(engine=innodb) finished...
2018/05/23 14:19:31 create table benchyou1(engine=innodb) finished...
2018/05/23 14:19:32 create table benchyou2(engine=innodb) finished...
...
...

2018/05/23 14:20:09 create table benchyou29(engine=innodb) finished...
2018/05/23 14:20:10 create table benchyou30(engine=innodb) finished...
2018/05/23 14:20:11 create table benchyou31(engine=innodb) finished...
```

## step5  开始压测

### 5.1 关闭分布式事务，关闭审计功能，随机写

radon master节点执行指令
 参数说明：

 `twopc-enable` 是分布式事务开关，设置为 `false`

 `audit-mode` 是审计功能开关，设置为`N`:

 `allowip`是设置允许登入到radon的IP地址。
 
```
$ curl -i -H 'Content-Type: application/json' -X PUT -d '{"max-connections":1024, "max-result-size":1073741824, "ddl-timeout":3600, "query-timeout":600, "twopc-enable":false, "allowip": ["192.168.0.28", "192.168.0.14", "192.168.0.15"], "audit-mode": "N"}' http://192.168.0.16:8080/v1/radon/config
```

执行以下命令
`Note`: 以下只演示如何压测，这里只设置了64个并发线程，具体的线程数可根据你自己的环境设置，比如设置为512或者更高。

```
$ ./benchyou  --mysql-host=192.168.0.16 --mysql-port=3308 --mysql-user=root --mysql-password=123456 --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=64 --read-threads=0 --max-request=10000000 random
```

执行过程（需要一点时间，最后输出平均值）：
```
time            thds              tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[1s]         [r:0,w:64,u:0,d:0]  0        0       0       0      NaN      0      NaN       0.00    NaN       0.00    NaN      NaN     0       0         NaN        NaN          0
....
....
----------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                        r-rsp(ms)              total-number
[3602s]      1893     1893    0       0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:0.01,min:0.00,max:1385.86]  [avg:NaN,min:0.00,max:0.00]      6818994
```

### 5.2 关闭分布式事务，关闭审计功能，随机读

由于5.1步骤已经关闭分布式事务和审计功能，这里直接执行指令（这里读线程数设置为128，同上，具体的线程数皆可根据你自己的环境进行设置）：
```
$ ./benchyou  --mysql-host=192.168.0.16 --mysql-port=3308 --mysql-user=root --mysql-password=123456 --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=0 --read-threads=128 --max-request=10000000 random
```

执行过程（需要一点时间，最后输出平均值）：
```
time            thds               tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[1s]         [r:128,w:0,u:0,d:0]  0        0       0       0      NaN      0      NaN       0.00    NaN       0.00    NaN      NaN     0       0         NaN        NaN          0
...
...
----------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                    r-rsp(ms)              total-number
[2934s]      3408     0       3408    0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:NaN,min:0.00,max:0.00]  [avg:0.01,min:0.00,max:1813.36]      10000138
```

### 5.3 关闭分布式事务，关闭审计功能，随机混合读写

由于5.1步骤已经关闭分布式事务和审计功能，这里直接执行指令：
```
$ ./benchyou  --mysql-host=192.168.0.16 --mysql-port=3308 --mysql-user=root --mysql-password=123456 --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=64 --read-threads=64 --max-request=10000000 random
```

执行过程（需要一点时间，最后输出平均值）：
```	
time            thds               tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[1s]         [r:64,w:64,u:0,d:0]  0        0       0       0      NaN      0      NaN       0.00    NaN       0.00    NaN      NaN     0       0         NaN        NaN          0
....
....
----------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                        r-rsp(ms)              total-number
[3604s]      1739     508     1230    0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:0.03,min:0.00,max:6936.69]  [avg:0.01,min:0.00,max:6200.80]      6269848		
```


### 5.4 开启分布式事务，关闭审计功能, 随机混合读写

在radon master节点下，打开分布式事务开关: `twopc-enable` 设置为 `true`

```
$ curl -i -H 'Content-Type: application/json' -X PUT -d '{"max-connections":1024, "max-result-size":1073741824, "ddl-timeout":3600, "query-timeout":600, "twopc-enable":true, "allowip": ["192.168.0.28", "192.168.0.14", "192.168.0.15"], "audit-mode": "N"}' http://192.168.0.16:8080/v1/radon/config
```

执行指令：
```
$ ./benchyou  --mysql-host=192.168.0.16 --mysql-port=3308 --mysql-user=root --mysql-password=123456 --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=64 --read-threads=64 --max-request=10000000 random
``` 

执行过程（需要一点时间，最后输出平均值）：
```
time            thds               tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[1s]         [r:64,w:64,u:0,d:0]  0        0       0       0      NaN      0      NaN       0.00    NaN       0.00    NaN      NaN     0       0         NaN        NaN          0
...
...
----------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                        r-rsp(ms)              total-number
[3526s]      2836     967     1869    0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:0.02,min:0.00,max:3808.16]  [avg:0.01,min:0.00,max:3783.29]      10001004
```

### 5.5 开启分布式事务，开启审计功能, 随机混合读写

在radon master节点下 : `twopc-enable` 设置为 `true`, `audit-mode`设置为`A`
```
$ curl -i -H 'Content-Type: application/json' -X PUT -d '{"max-connections":1024, "max-result-size":1073741824, "ddl-timeout":3600, "query-timeout":600, "twopc-enable":true, "allowip": ["192.168.0.28", "192.168.0.14", "192.168.0.15"], "audit-mode": "A"}' http://192.168.0.16:8080/v1/radon/config
```

执行benchyou指令：
```
$ ./benchyou  --mysql-host=192.168.0.16 --mysql-port=3308 --mysql-user=root --mysql-password=123456 --oltp-tables-count=32  --mysql-table-engine=innodb  --write-threads=64 --read-threads=64 --max-request=10000000 random
```

执行过程（需要一点时间，最后输出平均值）：
```
time            thds               tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op  freeMB  cacheMB   w-rsp(ms)  r-rsp(ms)    total-number
[1s]         [r:64,w:64,u:0,d:0]  0        0       0       0      NaN      0      NaN       0.00    NaN       0.00    NaN      NaN     0       0         NaN        NaN          0
....
....
----------------------------------------------------------------------------------------------avg---------------------------------------------------------------------------------------------
time          tps     wtps    rtps    rio    rio/op   wio    wio/op    rMB     rKB/op    wMB     wKB/op   cpu/op            w-rsp(ms)                        r-rsp(ms)              total-number
[3602s]      2691     920     1770    0      0.00     0      0.00      0.00    0.00      0.00    0.00     0.00    [avg:0.02,min:0.00,max:4281.83]  [avg:0.01,min:0.00,max:3794.18]      9693881
```

