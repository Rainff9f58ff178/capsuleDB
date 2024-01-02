# capsuleDB - query thousands of rows in century
This is my database as graduation project.

## implementing:
    - imp MVVC
    - imp update,delete
    - intergrete multh thread execute 
    - improve expression execute performance
    - imp multi file .

## implemented:
    - TableScan
    - HashJoin
    - Limit
    - Sort
    - Filter
    - explain
    - subquery
    - aggregate
    - c++20 coroutine scheduler
    - push-base execute. column execute engine.
    - column store engine. page base.


## compile 
```shell
git clone https://github.com/Rainff9f58ff178/capsuleDB && cd capsuleDB && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Debug .. && make -j16 

```
## boot
```shell
./capsule-server 127.0.0.1 8080

./capsule-client 127.0.0.1 8080
```


```sql
-- input \st to show tables

-- input \q to quit

-- only support varchar and int column

-- execute 'select * from database_info;' show more infomation


Capsule >select * from c;
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



