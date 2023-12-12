# capsuleDB - query thousands of rows in century
This is my database as graduation project.

## implementing:
    - 1.c++20 coroutine scheduler
    - 2.push-base execute. column execute engine.
    - 3.column store engine. page base.

## implemented:
    - TableScan
    - HashJoin
    - Limit
    - Sort
    - Filter
    - explain

```sql
-- input \st to show tables

-- input \q to quit

-- only support varchar and int column

-- execute 'select * from database_info;' show more infomation


Capsule >insert into c values('💩🤝💦👃👴🐍🐔，💊');
+------------------------------------------+
|                                    c.cola|
+------------------------------------------+
|                        💩🤝💦👃👴🐍🐔，💊|
+------------------------------------------+

1 record  in: 0.000393 second 1 chunk
Capsule >select * from database_info as a;
+---------------+---------------+--------------------------------------------+------------------+
|a.database_name|       a.author|                               a.github_addr|     a.description|
+---------------+---------------+--------------------------------------------+------------------+
|      capsuleDB|Rainff9f58ff178|https://github.com/Rainff9f58ff178/capsuleDB|nice to meet you !|
+---------------+---------------+--------------------------------------------+------------------+

1 record  in: 0.000758 second 1 chunk

```



