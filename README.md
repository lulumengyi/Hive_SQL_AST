# Hive_SQL_AST
利用Druid SQL Parser解析HiveSQL日志，自动构建字段级别的血缘关系及主外键的自动抽取 

# Druid
Druid是alibaba开源的一个JDBC组件库，包括数据库连接池、SQL Parser等组件，DruidDataSource官方宣称是最好的数据库连接池，Druid能够提供强大的监控和扩展功能。
具体信息可参考官方wiki：https://github.com/alibaba/druid/wiki/%E9%A6%96%E9%A1%B5
# Druid SQL Parser
在这个项目中，只用到SQL Parser组件，在这里主要详解我们是如何利用SQL Parser进行解析sql日志信息。
## 简介
首先了解下SQL Parser。SQL Parser是Druid的一个重要组成部分，Druid内置使用SQL Parser来实现防御SQL注入（WallFilter）、合并统计没有参数化的SQL(StatFilter的mergeSql)、SQL格式化、分库分表。

具体的wiki信息：https://github.com/alibaba/druid/wiki/SQL-Parser

先来了解一下Druid SQL parser的结构，它主要分三个模块：

   - Parser
   - AST
   - Visitor

### Parser
parser是将输入文本转换为ast（抽象语法树），parser有包括两个部分，Parser和Lexer，其中Lexer实现词法分析，Parser实现语法分析。
### AST
AST是Abstract Syntax Tree的缩写，也就是抽象语法树。AST是parser输出的结果。我们通过下面的语句来产生AST：
```java
final String dbType = JdbcConstants.MYSQL; // 可以是ORACLE、POSTGRESQL、SQLSERVER、ODPS等
String sql = "select * from t";
List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
```
第一句是进行数据库连接，得到数据库类型以便之后的解析，第二句示例sql代码，然后我们利用SQLUtils的parseStatement产生List<SQLStatement>。
常用的SQLStatemment包括SELECT/UPDATE/DELETE/INSERT，例如这个sql就是属于 SQLSelectStatement ，SQLStatement你可以简单理解为1条SQL语句。
```java
class SQLSelectStatement implements SQLStatement {
    SQLSelect select;
}
```
在之后SQLSelectStatement包含一个SQLSelect，SQLSelect包含一个SQLSelectQuery，都是组成的关系。SQLSelectQuery有主要的两个派生类，分别是SQLSelectQueryBlock和SQLUnionQuery。
然后我们根据定义再继续看SQLSelect的定义，
```java
class SQLSelect extends SQLObjectImpl { 
    SQLWithSubqueryClause withSubQuery;
    SQLSelectQuery query;
}

interface SQLSelectQuery extends SQLObject {}

class SQLSelectQueryBlock implements SQLSelectQuery {
    List<SQLSelectItem> selectList;
    SQLTableSource from;
    SQLExprTableSource into;
    SQLExpr where;
    SQLSelectGroupByClause groupBy;
    SQLOrderBy orderBy;
    SQLLimit limit;
}

class SQLUnionQuery implements SQLSelectQuery {
    SQLSelectQuery left;
    SQLSelectQuery right;
    SQLUnionOperator operator; // UNION/UNION_ALL/MINUS/INTERSECT
}
```
SQLSelcet包含一个SQLSelectQuery，在SQLSelectQuery里我们看到它包含selectList，from，into，where等关键字，我们就
可以将sql解析了，比如这个简单的sql= "select id from t"，它的selectlist为 id，这里的from t是一个SQLExprTableSource，
其中expr是一个name=t的SQLIdentifierExpr。

当然我们可以清晰的知道id是属于t表的，但是在我们的sql日志中，sql语句没有这么简单，在数据仓库中通过一系列的
调用，比如在一张全量用户信息表中想得到所有用户年龄信息，我们提取出来构成一张表，然后在年龄信息表中我们又想得到出不同年龄段的职业信息，再构成一张表，这样下去我们
会构成很多张临时表，方便进行一些业务操作。但是这些字段其实都是来源于最开始的用户信息表的，当我们想知道这临时表中某个字段到底是来源于哪个物理表的，我们
通过sql的语法解析树一层层解析，可以自动的找到它的血缘关系。

比较复杂的sql示例：(为了保护公司数据，代码进行了修改）
```sql
create table tmp.tmp_a_supp_achievement_an_mom_001 as
select a1.dim_day_txdate
       ,a.a_pin
      ,sum(coalesce(b.amount,0)) as total_amount
      ,sum(coalesce(c.refund_amt,0)) as refund_amt
      ,sum(os_prcp_amt)os_prcp_amt
from
   (select dim_day_txdate
    from dmv.dim_day
    where dim_day_txdate>=concat(cast(year('2018-05-15')-1 as string),'-',substring('2018-05-15',6,2),'-01') and dim_day_txdate<='2018-05-15'
    )a1
join
   (select distinct a_pin
          ,product_type
    from dwd.dwd_as_qy_cust_account_s_d
    where dt ='2018-05-15' and product_type='20288'
    )a
left outer join
   (select substring(tx_time,1,10) as time 
          ,sum(order_amt) as amount 
          ,a_pin
    from DWD.dwd_actv_as_qy_iou_receipt_s_d-------
    where a_order_type='20096' -
      and a_pin not in ('vep_test','VOPVSP测试','VOPVSP测试_1','测试号','2016联通测试号','pxpx01','pxpx02',
                                  'i000','i001','测试','测试aa01','测试aa02','px01','px02',
                                  'test','test01','px031901','px031902','多级审核测试admin','邮政测试2015','中石油积分兑换-测试','买卖宝测试王','mengmengda111','ZHAOGANGWANG1809','ZHAOGANGWANGC1000508',
                                  '差旅测试01','差旅测试03','差旅测试04','差旅测试02','差旅测试06','差旅测试05','jc_test1','大连航天测试','大客户金采测试','移动测试账号1','中国联通测试','云积分商城测试'
                                  ,'多级审核测试采购08','多级审核测试采购05','国电物流有限公司测试')
      and dt='2018-05-15'
    group by substring(tx_time,1,10),a_pin
    )b on cast(a.a_pin as string)=cast(b.a_pin as string) and a1.dim_day_txdate=b.time
left outer join
   (select substring(refund_time,1,10) as refund_time
          ,a_pin
          ,sum(refund_amt)as refund_amt
    from DWD.dwd_as_qy_iou_refund_s_d
    where refund_status='20090'
      and dt='2018-05-15'
      and a_order_no <> '12467657248' 
      and a_refund_no <> '1610230919767139947'  
    group by substring(refund_time,1,10),a_pin
    )c on cast(a.a_pin as string)=cast(c.a_pin as string) and a1.dim_day_txdate=c.refund_time
left outer join
(select dt,a_pin,sum(os_prcp_amt) as os_prcp_amt  from dwd.dwd_as_qy_cycle_detail_s_d where dt>=concat(substr('2018-05-15',1,7),'-01') and dt<='2018-05-15' group by dt,a_pin)e on cast(a.jd_pin as string)=cast(e.a_pin as string) and a1.dim_day_txdate=e.dt
group by a1.dim_day_txdate,a.a_pin
; 
```

所以从上面的sql中，进行我们的解析可以得到dim_day_txdate字段来源于dmv.dim_day，a_pin来源于dwd.dwd_as_qy_cust_account_s_d，total_amount是
来源于DWD.dwd_actv_as_qy_iou_receipt_s_d中的(order_amt)经过sum之后得到amount 再通过sum和coleace操作得到的，这系列的血缘关系变化我们将用链表方式进行存储，得到最终的血缘关系。

## 代码结构如下：
```
dw-column-level-lineage/                                                         # 工程根目录
├── src/                           
|   ├─ main                    
|   |  ├─ lineage
|   |  |     ├─ analysis
|   |  |     |      ├─ ColumnLineageAnalyzer.java                                 #解析主函数
|   |  |     ├─ domains
|   |  |     |      ├─ basic                                                      #数据结构的基本定义
|   |  |     |      |    ├─ Column.java
|   |  |     |      |    ├─ ColumnType.java
|   |  |     |      |    ├─ Database.java
|   |  |     |      |    ├─ DatabaseType.java
|   |  |     |      |    ├─ Table.java
|   |  |     |      |    ├─ TableType.java
|   |  |     |      |    ├─ ForeignKeys.java
|   |  |     |      ├─ lineage                                                    # 血缘关系链表的定义
|   |  |     |      |    ├─ table
|   |  |     |      |    |     ├─ TableColumnLineage.java
|   |  |     |      |    ├─ column
|   |  |     |      |    |     ├─ ColumnLineageColumnNode.java
|   |  |     |      |    |     ├─ ColumnLineageColumnNodeType.java
|   |  |     |      |    |     ├─ ColumnLineageRelationNodeType.java
|   |  |     |      |    |     ├─ ColumnLineageRelationNode.java
|   |  |     |      |    |     ├─ ColumnLineageUtils.java
|   |  |     ├─ utils                                                            # 数据处理文件
|   |  |     |      ├─ ProcessUnparsedSQL.java
|   |  |     |      ├─ SchemaExtractor.java
|   |  |     |      ├─ SchemaLoader.java
|   |  |     |      ├─ SQLExtractor.java
|   |  ├─ resource    
|   |  |     ├─ log4j2.xml
|   ├─ test                                                                      # 单元测试文件
├── .gitignore                                                                   # Git Ignore 文件
├── pox.ml                                                                       # maven配置文件
            
```
