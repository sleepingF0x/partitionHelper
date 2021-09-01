# PartitionHelper
MYSQL自动创建/删除分区脚本
### 安装
下载代码
```shell
git clone https://github.com/sleepingF0x/partitionHelper.git
```
创建虚拟环境
```
sh create_venv.sh
```


### 对表的时间字段（类型：datetime）基于月/天进行分区

表结构示例：

```MYSQL
CREATE TABLE range_datetime(
    id INT,
    hiredate DATETIME
)
PARTITION BY RANGE (TO_DAYS(hiredate) ) (
    PARTITION p20151202 VALUES LESS THAN ( TO_DAYS('20151202') ),
    PARTITION p20151203 VALUES LESS THAN ( TO_DAYS('20151203') ),
    PARTITION p20151204 VALUES LESS THAN ( TO_DAYS('20151204') ),
    PARTITION p20151205 VALUES LESS THAN ( TO_DAYS('20151205') ),
    PARTITION p20151206 VALUES LESS THAN ( TO_DAYS('20151206') ),
    PARTITION p20151207 VALUES LESS THAN ( TO_DAYS('20151207') ),
    PARTITION p20151208 VALUES LESS THAN ( TO_DAYS('20151208') ),
    PARTITION p20151209 VALUES LESS THAN ( TO_DAYS('20151209') ),
    PARTITION p20151210 VALUES LESS THAN ( TO_DAYS('20151210') ),
    PARTITION p20151211 VALUES LESS THAN ( TO_DAYS('20151211') )
);
```
执行脚本：

```shell
python3 range_by_time.py -H 127.0.0.1 -P 3306 -d my_database -u my_username -p my_password -r 3 -a 3 -b daily -t my_table
```

* -H : MYSQL 数据库服务ip地址
* -P : MYSQL数据库服务端口
* -d : 数据库名
* -t : 表名
* -u : MYSQL数据库服务用户名
* -p : MYSQL数据库服务密码
* -b : 分区单位（按月/按日）
* -r : 需要删除的分区表数量
* -a : 需要添加的分区表数量
* -f : 时间转换函数 TO_DAYS/TIMESTAMP

### 示例

如今天是2020-10-15日，下面这个命令将会删除3天前的分区p20201012（2020-10-12以前的数据），同时添加后3天的分区p20201016、p20201017、p20201018。

    python3 range_by_time.py -H 127.0.0.1 -P 3306 -d my_database -u my_username -p my_password -r 3 -a 3 -b daily -t my_table

### 添加到定时任务

1. 编辑定时任务

```shell
crontab -e
```
2. 每天10点执行脚本

    00 10 * * * 脚本路径/venv/bin/python 脚本路径/range_by_time.py -H 127.0.0.1 -d database -u username -p password -r 3 -a 3 -b daily -t table >> 脚本路径/partition.log

定时任务说明

`*　　*　　*　　*　　*　　command`

分　 时　  日　 月　 周　  命令

- 第1列表示分钟1～59 每分钟用*或者* /1表示

- 第2列表示小时1～23（0表示0点）

- 第3列表示日期1～31

- 第4列表示月份1～12

- 第5列标识号星期0～6（0表示星期天）

- 第6列要运行的命令
