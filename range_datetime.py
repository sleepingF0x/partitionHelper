from datetime import datetime
from optparse import OptionParser

import pymysql
from dateutil.relativedelta import relativedelta


def main(options):
    curr = datetime.now()
    print("Current time : ", curr)
    partitionQuery = "SELECT PARTITION_NAME FROM information_schema.partitions WHERE TABLE_SCHEMA = '%s' " \
                     "AND TABLE_NAME = '%s' AND PARTITION_NAME IS NOT NULL"
    removeQuery = "ALTER TABLE %s DROP PARTITION %s"
    addQuery = "ALTER TABLE %s ADD PARTITION (PARTITION p%s VALUES LESS THAN (TO_DAYS('%s')))"

    db_conn = pymysql.connect(host=options.host, port=int(options.port), user=options.user, password=options.password,
                              database=options.db)
    db_cur = db_conn.cursor(pymysql.cursors.DictCursor)

    db_cur.execute(partitionQuery % (options.db, options.table))
    tbl_partitions = db_cur.fetchall()

    remove_partitions = []
    add_partitions = []

    if options.basis == "daily":
        remove = datetime.now() - relativedelta(days=options.removePart, hour=0, minute=0, second=0)
        remove_partition = remove.strftime("p%Y%m%d")
        for partition in tbl_partitions:
            if partition['PARTITION_NAME'] <= remove_partition:
                remove_partitions.append(partition['PARTITION_NAME'])
        for i in range(options.addPart):
            add = datetime.now() + relativedelta(days=i + 1, hour=0, minute=0, second=0)
            add_partitions.append(add.strftime("%Y%m%d"))

    elif options.basis == "monthly":
        remove = datetime.now() - relativedelta(months=options.removePart, day=1, hour=0, minute=0, second=0)
        remove_partition = remove.strftime("p%Y%m%d")
        for partition in tbl_partitions:
            if partition['PARTITION_NAME'] <= remove_partition:
                remove_partitions.append(partition['PARTITION_NAME'])
        for i in range(options.addPart):
            add = datetime.now() + relativedelta(months=i + 1, day=1, hour=0, minute=0, second=0)
            add_partitions.append(add.strftime("%Y%m%d"))

    else:
        return

    for remove in remove_partitions:
        db_cur.execute(removeQuery % (options.table, remove))
        print("remove partition %s" % remove)
    for add in add_partitions:
        db_cur.execute(addQuery % (options.table, add, add))
        print("add partition p%s" % add)

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
                      help="a partition to be added ")
    parser.add_option("-r", "--remove", action="store", type="int", dest="removePart",
                      help="a partition to be removed")
    parser.add_option("-b", "--basis", action="store", type="choice", dest="basis", default="daily",
                      choices=['daily', 'monthly'], help="time basis")

    (args_options, _) = parser.parse_args()
    try:
        main(args_options)
    except Exception as e:
        print(e)
