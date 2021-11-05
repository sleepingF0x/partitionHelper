from datetime import datetime
from optparse import OptionParser

import pymysql
from dateutil.relativedelta import relativedelta


def main(options):
    curr = datetime.now()
    print("Current time : ", curr)
    partitionQuery = "SELECT PARTITION_NAME FROM information_schema.partitions WHERE TABLE_SCHEMA = '{db}' " \
                     "AND TABLE_NAME = '{table}' AND PARTITION_NAME IS NOT NULL"
    removeQuery = "ALTER TABLE `{table}` DROP PARTITION `{part}`"
    addQuery = "ALTER TABLE `{table}` ADD PARTITION (PARTITION `{part}` VALUES LESS THAN ({func}('{dt}')))"

    db_conn = pymysql.connect(host=options.host, port=int(options.port), user=options.user,
                              password=options.password, database=options.db)
    db_cur = db_conn.cursor(pymysql.cursors.DictCursor)
    sql = partitionQuery.format(db=options.db, table=options.table)
    db_cur.execute(sql)
    tbl_partitions = db_cur.fetchall()

    remove_partitions = []
    add_partitions = []

    if options.basis == "daily":
        if options.removePart:
            remove = datetime.now() - relativedelta(days=options.removePart, hour=0, minute=0, second=0)
            remove_partition = remove.strftime("p_%Y%m%d")
            for partition in tbl_partitions:
                if partition['PARTITION_NAME'] <= remove_partition:
                    remove_partitions.append(partition['PARTITION_NAME'])
        for i in range(options.addPart):
            part_name = 'p_' + (datetime.now() + relativedelta(days=i + 1, hour=0, minute=0, second=0)).strftime(
                "%Y%m%d")
            time_lt = (datetime.now() + relativedelta(days=i + 2, hour=0, minute=0, second=0)).strftime("%Y-%m-%d")
            add_partitions.append((part_name, time_lt))

    elif options.basis == "monthly":
        if options.removePart:
            remove = datetime.now() - relativedelta(months=options.removePart, day=1, hour=0, minute=0, second=0)
            remove_partition = remove.strftime("p_%Y%m%d")
            for partition in tbl_partitions:
                if partition['PARTITION_NAME'] <= remove_partition:
                    remove_partitions.append(partition['PARTITION_NAME'])
        for i in range(options.addPart):
            part_name = 'p_' + (datetime.now() + relativedelta(months=i + 1, day=1, hour=0, minute=0, second=0)).\
                strftime("%Y%m")
            time_lt = (datetime.now() + relativedelta(months=i + 2, day=1, hour=0, minute=0, second=0)).\
                strftime("%Y-%m-%d")
            add_partitions.append((part_name, time_lt))
    else:
        return

    for remove in remove_partitions:
        sql = removeQuery.format(table=options.table, part=remove)
        db_cur.execute(sql)
        print("remove partition %s" % remove)
    for part_name, time_lt in add_partitions:
        sql = addQuery.format(table=options.table, func=options.func, part=part_name, dt=time_lt)
        db_cur.execute(sql)
        print("add partition %s" % part_name)

    db_cur.close()
    db_conn.close()
    print("Complete Work")


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-H", "--host", action="store", type="string", dest="host",
                      help="MySQL Server Address")
    parser.add_option("-P", "--port", action="store", type="string", dest="port",
                      help="MySQL port")
    parser.add_option("-d", "--database", action="store", type="string", dest="db",
                      help="MySQL database name")
    parser.add_option("-t", "--table", action="store", type="string", dest="table",
                      help="MySQL Table")
    parser.add_option("-u", "--user", action="store", type="string", dest="user",
                      help="MySQL User Name")
    parser.add_option("-p", "--password", action="store", type="string", dest="password",
                      help="MySQL User Password")
    parser.add_option("-a", "--add", action="store", type="int", dest="addPart",
                      help="a partition to be added ", default=0)
    parser.add_option("-r", "--remove", action="store", type="int", dest="removePart",
                      help="a partition to be removed", default=0)
    parser.add_option("-f", "--func", action="store", type="choice", dest="func", default="TO_DAYS",
                      choices=['TO_DAYS', 'UNIX_TIMESTAMP'], help="TO_DAYS: DATE, DATETIME, UNIX_TIMESTAMP: TIMESTAMP")
    parser.add_option("-b", "--basis", action="store", type="choice", dest="basis", default="daily",
                      choices=['daily', 'monthly'], help="time basis")

    (args_options, _) = parser.parse_args()
    try:
        main(args_options)
    except Exception as e:
        print(e)
