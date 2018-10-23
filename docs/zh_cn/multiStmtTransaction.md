[TOC]

----------------------------------------------------------

# 多语句事务支持调研

# overview

开发多语句事务支持使用

## 1 注意事项

## 关键字
 begin/start transaction
 rollback 
 commit 
 savepoint（不做）
 


## 触发事务因素

 `官方文档`:

**下面这些语句不触发事务(不会修改引擎数据，因此它们不会启动语句事务，也不会影响正常事务)**
1、管理语句 如：RESET SLAVE
2、状态信息语句: 例如 SHOW STATUS

**下面这些语句自动触发事务：**
The normal transaction encloses all statement transactions that are issued between its beginning and its end. In autocommit mode, the normal transaction is equivalent to the statement transaction.（normal事务包含在其开始和结束之间发出的所有语句事务。 在自动提交模式下，normal事务等同于语句事务。）

Similarly, DDL statements are not transactional, and therefore a transaction is (almost) never started for a DDL statement. But there's a difference between a DDL statement and an administrative statement: the DDL statement always commits the current transaction (if any) before proceeding; the administrative statement doesn't.（类似地，DDL语句不是事务性的，因此（几乎）从不为DDL语句启动事务。 但是DDL语句和管理语句之间存在差异：DDL语句总是在继续之前提交当前事务（如果有）；管理语句则不会。）

  `官方文档6.4`：
 `normal transaction(多语句,非单语句事务) commit方式`:
1、当用户发出SQL COMMIT语句时
2、隐式地，当服务器开始处理DDL语句或SET AUTOCOMMIT = {0 | 1}语句时（`当前radondb不支持set，不考虑这个case`）

`normal transaction回滚`：
1、当用户发出SQL ROLLBACK语句时
2、当其中一个存储引擎通过设置thd-> transaction_rollback_request请求回滚时（`？？`）



`之前记录的一些明确的点`：
**测试多个session一些kill掉的情况**

**binlog记录到时候begin不记录读，记录写，在执行commit的时候一并从内存写入radon节点切换，事务执行（也不考虑这个崩溃问题)**

 **TODO**:Considering master node may be crashed when we executing a multi-statement transaction（目前不考虑崩溃）


### 流程图

`ExecuteMultiStmtBegin/ExecuteBegin函数`


```flow
st=>start: start
e=>end

cond=>condition:  spanner.isTwoPC()
op=>operation: print error msg
op1=>operation: get current session`s txn
op16=>operation: 结果集设为空,返回

cond1=>condition: txn == nil ?
cond2=>condition: 执行commit,commit失败？
op3=>operation: create txn
op4=>operation: log error
op5=>operation: txn.Finish() 
op6=>operation: sessions.TxnUnBinding(session)
cond3=>condition: 执行txn.Rollback()，失败？
op7=>operation: txn.Finish() 
op8=>operation: sessions.TxnUnBinding(session)
op9=>operation: log error
op10=>operation: return
op11=>operation: set txn limits
op12=>operation:  sessions.MultiStmtTxnBinding(session, txn, node, query)
cond4=>condition: 事务开始，txn.Begin(),begin失败？
op13=>operation: 结果集设为空,返回
op14=>operation: sessions.MultiStateTxnUnBinding(session, false)
op15=>operation: log error

st->cond
cond(yes,right)->op->op16
cond(no)->op1->cond1
cond1(no)->op3
cond1(yes, right)->cond2
cond2(no)->op5->op6
cond2(yes,right)->op4->cond3
cond3(no)->op7->op8->op10
cond3(yes,right)->op9
op3->op11->op12->cond4
cond4(yes,right)->op15->op7->op8->op10
cond4(no)->op14->op13->e
```


`ExecuteMultiStatRollback()/ExecuteRollback()函数`:


```flow
st=>start: start
e=>end

cond=>condition:  spanner.isTwoPC()
op=>operation: print error msg
op1=>operation: 结果集设为空,返回
op2=>operation: get current session`s txn
cond1=>condition: 事务txn==nil ?
cond2=>condition: 执行Rollback()，Rollback失败 ？
op3=>operation: print error
op4=>operation: txn.Finish()
op5=>operation: 返回nil, err
op6=>operation: sessions.MultiStateTxnUnBinding(session, true)


st->cond
cond(yes,right)->op->op1
cond(no)->op2->cond1
cond1(yes, right)->op1
cond1(no)->cond2
cond2(yes,right)->op3->op5
cond2(no)->op6->op4->op1	
```

`ExecuteMultiStmtCommit/ExecuteCommit函数` 流程和rollback类似


