Table of Contents
=================
   * [overview](#overview)
   * [版本(5.7-Radon-1.0)](#版本57-radon-10)
      * [1 feat： 新功能（feature）](#1-feat-新功能feature)
         * [1.1 select 支持表别名功能](#11-select-支持表别名功能)
      * [2 fix：修补bug](#2-fix修补bug)
      * [3 docs：文档（documentation）](#3-docs文档documentation)
      * [4 style： 格式（不影响代码运行的变动）](#4-style-格式不影响代码运行的变动)
      * [5 refactor：重构（即不是新增功能，也不是修改bug的代码变动）](#5-refactor重构即不是新增功>能也不是修改bug的代码变动)
      * [6 test：增加测试](#6-test增加测试)
      * [7 chore：构建过程或辅助工具的变动](#7-chore构建过程或辅助工具的变动)
      * [8 perf：代码调整以提高性能（performance）](#8-perf代码调整以提高性能performance)
      * [9 revert：撤销以前的 commit](#9-revert撤销以前的-commit)

# overview

该文档用于追踪radon的commit记录（中文版本），主要用于内部开发人员使用。commit类别以commit提交规范的HEAD type为准，英文版本的changelog必须记录feat和fix，其它可选。commit提交规范详见[commit_message_zh.md](commit_message_zh.md)

# 版本(5.7-Radon-1.0)

## 1 feat： 新功能（feature）

### 1.1 select 支持表别名功能

[pr链接](https://github.com/radondb/radon/pull/50) 
**描述**：原代码在处理含有表别名的select语句时，scatter到各个子节点的子sql别名已经替换回原表名，导致子sql在backend执行时候会报错。比如原始sql：

```
select t2.a from t1 as t2;
```

处理之后的某条子sql:

```
select t2.a from db.t1_0000 as t1;
```
调整代码之后执行结果为：
```
select t2.a from db.t1_0000 as t2;
```


## 2 fix：修补bug

## 3 docs：文档（documentation）

## 4 style： 格式（不影响代码运行的变动）

## 5 refactor：重构（即不是新增功能，也不是修改bug的代码变动）

## 6 test：增加测试

## 7 chore：构建过程或辅助工具的变动

## 8 perf：代码调整以提高性能（performance）

## 9 revert：撤销以前的 commit
