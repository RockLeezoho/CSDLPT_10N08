#!/usr/bin/python2.7
#
# Interface for the assignment
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        host='localhost',
        password=password
    )


def create_db(dbname):
    """
    Create a new database if it doesn't exist.
    """
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname = %s", (dbname,))
    if cur.fetchone()[0] == 0:
        cur.execute('CREATE DATABASE %s' % dbname)
    else:
        print('A database named {0} already exists'.format(dbname))
    cur.close()
    con.close()


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Load ratings data from file into table.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            userid INTEGER,
            extra1 CHAR,
            movieid INTEGER,
            extra2 CHAR,
            rating FLOAT,
            extra3 CHAR,
            timestamp BIGINT
        );
    """)
    with open(ratingsfilepath, 'r') as f:
        cur.copy_from(f, ratingstablename, sep=':')
    cur.execute(f"""
        ALTER TABLE {ratingstablename}
        DROP COLUMN extra1,
        DROP COLUMN extra2,
        DROP COLUMN extra3,
        DROP COLUMN timestamp;
    """)
    con.commit()
    cur.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Partition the ratingstablename into numberofpartitions partitions by rating range.
    """
    con = openconnection
    cur = con.cursor()
    range = 5.0 / numberofpartitions  # Mỗi phân vùng rộng range điểm
    lower = 0
    part = 0

    #Tao mot bang meta meta_range_part_info de luu tru so luong phan vung
    cur.execute("DROP TABLE IF EXISTS meta_range_part_info")
    cur.execute("CREATE TABLE meta_range_part_info (numberofpartitions INT)")
    cur.execute("TRUNCATE TABLE meta_range_part_info")
    cur.execute("INSERT INTO meta_range_part_info VALUES (" + str(numberofpartitions) + ") ")

    #Chen ban ghi vao cac phan vung khac nhau dua tren gia tri rating
    while lower < 5.0:
        if lower == 0:
            cur.execute("DROP TABLE IF EXISTS range_part"+str(part))
            cur.execute("CREATE TABLE range_part"+str(part)+" AS SELECT * FROM "+ratingstablename+" WHERE rating>="+str(lower)+" AND rating<="+str(lower+range)+";")
        else:
            cur.execute("DROP TABLE IF EXISTS range_part"+str(part))
            cur.execute("CREATE TABLE range_part"+str(part)+" AS SELECT * FROM "+ratingstablename+" WHERE rating>"+str(lower)+" AND rating<="+str(lower+range)+";")
        lower += range
        part += 1

    con.commit()
    cur.close()
        




def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Partition table using round-robin strategy.
    """
    con = openconnection
    cur = con.cursor()
    list_partitions = list(range(numberofpartitions))

    #Tao mot bang meta de luu tru so luong phan vung va phan vung cuoi cung ma du lieu duoc chen vao
    cur.execute("drop table if exists meta_rrobin_part_info")
    cur.execute("create table meta_rrobin_part_info (partition_number INT, numberofpartitions INT)")


    last=-1
    #Chen vao rrobin_partitions dua tren row_numbers
    for part in list_partitions:
        cur.execute("drop table if exists rrobin_part" + str(part))
        cur.execute("create table rrobin_part" + str(part) + " as select userid,movieid,rating from  (select userid,movieid,rating,row_number() over() as row_num from " + str(ratingstablename) + ") a where (a.row_num -1 + " + str(numberofpartitions) + ")% " + str(numberofpartitions) + " = " + str(part) )
        last=part
        

    #Cap nhat bang meta-data de luu tru phan vung cuoi cung noi du lieu duoc chen va tong so phan vung 
    cur.execute("truncate table meta_rrobin_part_info")	
    cur.execute("insert into meta_rrobin_part_info values (" + str(last) + "," + str(numberofpartitions) +") "  )

    con.commit()
    cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Insert into main table and correct round-robin partition.
    """
    con = openconnection
    cur = con.cursor()

    #Select the partition number where data was last inserted and the total number of partitions
    cur.execute("select partition_number,numberofpartitions from meta_rrobin_part_info")
    f = cur.fetchone()
    part = f[0]
    no_of_partitions = f[1]


    #Insert into the correct rrobin_partition  and then into the main ratings table as well	
    cur.execute("insert into rrobin_part" + str((part + 1)%no_of_partitions) + " values( " + str(userid) + "," + str(itemid) + "," + str(rating) + ") ")
    cur.execute("insert into ratings values( " + str(userid) + "," + str(itemid) + "," + str(rating) + ") ")
    part = (part + 1)%no_of_partitions

    #Update the partition number in the meta table
    cur.execute("truncate table meta_rrobin_part_info")	
    cur.execute("insert into meta_rrobin_part_info values (" + str(part) + "," + str(no_of_partitions) +") "  )

    con.commit()
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Insert into main table and correct range partition.
    """
    con = openconnection
    cur = con.cursor()

    #Select the total number of range partitions from the meta table	
    cur.execute("select numberofpartitions from meta_range_part_info")
    no_of_partitions = cur.fetchone()[0]

    range = 5.0/no_of_partitions
    low=0
    high=range
    part=0

    #Determine the correct partition
    while low<5.0:
        if low==0:
            if rating >= low and rating <= high:
                break
            part = part + 1
            low = high
            high = high + range
                    
        else:
            if rating > low and rating <= high:
                break
            part  = part + 1
            low = high
            high = high + range
            
    #Insert into the partition and main ratings table
    cur.execute("insert into range_part" + str(part) + " values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ") ")
    cur.execute("insert into ratings values( " + str(userid) + "," + str(itemid) + "," + str(rating) + ") ")
    con.commit()
    cur.close()


def count_partitions(prefix, openconnection):
    """
    Count partition tables with given prefix.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM pg_stat_user_tables
        WHERE relname LIKE %s
    """, (prefix + '%',))
    count = cur.fetchone()[0]
    cur.close()
    return count
