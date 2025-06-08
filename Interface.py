#!/usr/bin/python3
#
# Interface for the assignment
#

import psycopg2
import os
import sys

# Database configuration
DATABASE_NAME = 'csdlpt'
DB_USER = 'postgres'
DB_PASSWORD = '1234'  # Change this to your PostgreSQL password
DB_HOST = 'localhost'
DB_PORT = '5432'  # Default PostgreSQL port

def getopenconnection(user=DB_USER, password=DB_PASSWORD, dbname='postgres'):
    try:
        return psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=DB_HOST,
            port=DB_PORT
        )
    except psycopg2.OperationalError as e:
        print("Error: Could not connect to PostgreSQL database.")
        print("Please make sure:")
        print("1. PostgreSQL is installed and running")
        print("2. The password is correct")
        print("3. The database exists")
        print("\nDetailed error:", str(e))
        sys.exit(1)

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    # Tạo bảng nếu chưa có
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            userid INTEGER,
            movieid INTEGER,
            rating FLOAT
        )
    """)
    # Đọc file và insert từng dòng
    with open(ratingsfilepath, 'r') as f:
        for line in f:
            userid, movieid, rating, _ = line.strip().split('::')
            cur.execute(
                f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)",
                (int(userid), int(movieid), float(rating))
            )
    openconnection.commit()
    cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    # Xóa các bảng phân mảnh cũ nếu có
    for i in range(0, 100):
        cur.execute(f"DROP TABLE IF EXISTS range_part{i}")
    # Tính khoảng
    interval = 5.0 / numberofpartitions
    for i in range(numberofpartitions):
        cur.execute(f"""
            CREATE TABLE range_part{i} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            )
        """)
        min_rating = i * interval
        max_rating = min_rating + interval
        if i == 0:
            cur.execute(f"""
                INSERT INTO range_part{i}
                SELECT userid, movieid, rating FROM {ratingstablename}
                WHERE rating >= %s AND rating <= %s
            """, (min_rating, max_rating))
        else:
            cur.execute(f"""
                INSERT INTO range_part{i}
                SELECT userid, movieid, rating FROM {ratingstablename}
                WHERE rating > %s AND rating <= %s
            """, (min_rating, max_rating))
    openconnection.commit()
    cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    # Xóa các bảng phân mảnh cũ nếu có
    for i in range(0, 100):
        cur.execute(f"DROP TABLE IF EXISTS rrobin_part{i}")
    # Tạo bảng phân mảnh
    for i in range(numberofpartitions):
        cur.execute(f"""
            CREATE TABLE rrobin_part{i} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            )
        """)
    # Chia dữ liệu theo round robin
    cur.execute(f"SELECT userid, movieid, rating FROM {ratingstablename}")
    rows = cur.fetchall()
    for idx, row in enumerate(rows):
        part = idx % numberofpartitions
        cur.execute(f"INSERT INTO rrobin_part{part} (userid, movieid, rating) VALUES (%s, %s, %s)", row)
    openconnection.commit()
    cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    # Đếm số phân mảnh round robin
    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE 'rrobin_part%'")
    numberofpartitions = cur.fetchone()[0]
    # Đếm số dòng hiện có trong bảng chính TRƯỚC khi insert
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
    total_rows = cur.fetchone()[0]
    part = total_rows % numberofpartitions
    # Insert vào bảng chính
    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
    # Insert vào phân mảnh
    cur.execute(f"INSERT INTO rrobin_part{part} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
    openconnection.commit()
    cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    # Đếm số phân mảnh
    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE 'range_part%'")
    numberofpartitions = cur.fetchone()[0]
    interval = 5.0 / numberofpartitions
    part = int(rating / interval)
    if rating % interval == 0 and part != 0:
        part -= 1
    # Insert vào bảng chính
    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
    # Insert vào phân mảnh
    cur.execute(f"INSERT INTO range_part{part} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
    openconnection.commit()
    cur.close()

def create_db(dbname):
    """
    Create a database if it doesn't exist.
    """
    try:
        con = getopenconnection(dbname='postgres')
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        
        cur.execute(f"SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname='{dbname}'")
        count = cur.fetchone()[0]
        
        if count == 0:
            cur.execute(f'CREATE DATABASE {dbname}')
            print(f"Database {dbname} created successfully")
        else:
            print(f"Database {dbname} already exists")
        
        cur.close()
        con.close()
    except Exception as e:
        print("Error creating database:", str(e))
        sys.exit(1)

def count_partitions(prefix, openconnection):
    """
    Count the number of tables with the given prefix.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute(f"SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE '{prefix}%'")
    count = cur.fetchone()[0]
    cur.close()
    return count