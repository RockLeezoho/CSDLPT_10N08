#!/usr/bin/python3
#
# Interface for the assignment using MySQL
#

import mysql.connector

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='root', password='', dbname='mysql'):
    return mysql.connector.connect(user=user, password=password, host='localhost', database=dbname)


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Load ratings from file into table
    """
    create_db(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()
    cur.execute(f"CREATE TABLE IF NOT EXISTS {ratingstablename} (userid INT, movieid INT, rating FLOAT);")

    with open(ratingsfilepath, 'r') as file:
        for line in file:
            parts = line.strip().split(':')
            if len(parts) >= 5:
                userid = int(parts[0])
                movieid = int(parts[2])
                rating = float(parts[4])
                cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)",
                            (userid, movieid, rating))

    cur.close()
    con.commit()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()
    delta = 5.0 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'

    for i in range(numberofpartitions):
        minRange = i * delta
        maxRange = minRange + delta
        table_name = RANGE_TABLE_PREFIX + str(i)
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (userid INT, movieid INT, rating FLOAT)")
        if i == 0:
            cur.execute(f"INSERT INTO {table_name} SELECT * FROM {ratingstablename} WHERE rating >= {minRange} AND rating <= {maxRange}")
        else:
            cur.execute(f"INSERT INTO {table_name} SELECT * FROM {ratingstablename} WHERE rating > {minRange} AND rating <= {maxRange}")

    cur.close()
    con.commit()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    for i in range(numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (userid INT, movieid INT, rating FLOAT)")

    cur.execute(f"SELECT * FROM {ratingstablename}")
    rows = cur.fetchall()
    for index, row in enumerate(rows):
        table_index = index % numberofpartitions
        table_name = RROBIN_TABLE_PREFIX + str(table_index)
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)", row)

    cur.close()
    con.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)",
                (userid, itemid, rating))
    con.commit()

    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
    total_rows = cur.fetchone()[0]
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    index = (total_rows - 1) % numberofpartitions
    table_name = RROBIN_TABLE_PREFIX + str(index)
    cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)",
                (userid, itemid, rating))

    cur.close()
    con.commit()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'

    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    delta = 5.0 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index -= 1
    table_name = RANGE_TABLE_PREFIX + str(index)
    cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)",
                (userid, itemid, rating))

    cur.close()
    con.commit()


def create_db(dbname):
    """
    Tạo database nếu chưa tồn tại (cho MySQL)
    """
    con = getopenconnection(dbname='mysql')
    cur = con.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {dbname}")
    cur.close()
    con.close()


def count_partitions(prefix, openconnection):
    """
    Đếm số bảng có prefix nhất định (cho MySQL)
    """
    con = openconnection
    cur = con.cursor()
    cur.execute(f"SHOW TABLES LIKE '{prefix}%'")
    count = len(cur.fetchall())
    cur.close()
    return count
