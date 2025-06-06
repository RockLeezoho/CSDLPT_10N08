import traceback
import mysql.connector

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

# SETUP Functions
def createdb(dbname):
    """
    Tạo CSDL mới nếu chưa tồn tại trong MySQL.
    """
    con = getopenconnection()
    con.autocommit = True
    cur = con.cursor()

    # Check if database exists
    cur.execute("SHOW DATABASES LIKE %s", (dbname,))
    result = cur.fetchone()

    if not result:
        cur.execute("CREATE DATABASE `{}`".format(dbname))
    else:
        print('A database named "{}" already exists'.format(dbname))

    cur.close()
    con.close()

def delete_db(dbname):
    con = getopenconnection()
    con.autocommit = True
    cur = con.cursor()
    cur.execute("DROP DATABASE IF EXISTS `{}`".format(dbname))
    cur.close()
    con.close()

def deleteAllPublicTables(openconnection):
    cur = openconnection.cursor()
    cur.execute("SHOW TABLES")
    tables = cur.fetchall()
    for (table,) in tables:
        cur.execute("DROP TABLE IF EXISTS `{}`".format(table))
    cur.close()

def getopenconnection(user='root', password='', dbname='mysql'):
    config = {
        'user': user,
        'password': password,
        'host': 'localhost',
        'autocommit': True
    }
    if dbname:
        config['database'] = dbname
    return mysql.connector.connect(**config)

# --- Các hàm hỗ trợ ---
def getCountrangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    countList = []
    interval = 5.0 / numberofpartitions

    cur.execute("SELECT COUNT(*) FROM `{}` WHERE rating >= {} AND rating <= {}".format(ratingstablename, 0, interval))
    countList.append(int(cur.fetchone()[0]))

    lowerbound = interval
    for i in range(1, numberofpartitions):
        cur.execute("SELECT COUNT(*) FROM `{}` WHERE rating > {} AND rating <= {}".format(ratingstablename, lowerbound, lowerbound + interval))
        countList.append(int(cur.fetchone()[0]))
        lowerbound += interval

    cur.close()
    return countList

def getCountroundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    countList = []

    for i in range(0, numberofpartitions):
        cur.execute(
            "SELECT COUNT(*) FROM (SELECT *, (@rownum := @rownum + 1) AS rn FROM `{}` , (SELECT @rownum := 0) r) AS temp WHERE (rn - 1) % {} = {}".format(
                ratingstablename, numberofpartitions, i))
        countList.append(int(cur.fetchone()[0]))

    cur.close()
    return countList

def checkpartitioncount(cursor, expectedpartitions, prefix):
    cursor.execute("SHOW TABLES LIKE '{}%'".format(prefix))
    count = len(cursor.fetchall())
    if count != expectedpartitions:
        raise Exception('Expected {} partition tables, but found {}'.format(expectedpartitions, count))

def totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex):
    selects = []
    for i in range(partitionstartindex, n + partitionstartindex):
        selects.append('SELECT * FROM `{}`'.format(rangepartitiontableprefix + str(i)))
    cur.execute('SELECT COUNT(*) FROM ({}) AS T'.format(' UNION ALL '.join(selects)))
    count = int(cur.fetchone()[0])
    return count

def testrangeandrobinpartitioning(n, openconnection, rangepartitiontableprefix, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    with openconnection.cursor() as cur:
        if not isinstance(n, int) or n < 0:
            checkpartitioncount(cur, 0, rangepartitiontableprefix)
        else:
            checkpartitioncount(cur, n, rangepartitiontableprefix)
            count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
            if count < ACTUAL_ROWS_IN_INPUT_FILE:
                raise Exception("Completeness failed. Expected {}, got {}".format(ACTUAL_ROWS_IN_INPUT_FILE, count))
            if count > ACTUAL_ROWS_IN_INPUT_FILE:
                raise Exception("Disjointness failed. Expected {}, got {}".format(ACTUAL_ROWS_IN_INPUT_FILE, count))
            if count != ACTUAL_ROWS_IN_INPUT_FILE:
                raise Exception("Reconstruction failed. Expected {}, got {}".format(ACTUAL_ROWS_IN_INPUT_FILE, count))

def testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
    with openconnection.cursor() as cur:
        cur.execute(
            'SELECT COUNT(*) FROM `{}` WHERE {} = %s AND {} = %s AND {} = %s'.format(expectedtablename,
                                                                                     USER_ID_COLNAME,
                                                                                     MOVIE_ID_COLNAME,
                                                                                     RATING_COLNAME),
            (userid, itemid, rating)
        )
        count = int(cur.fetchone()[0])
        return count == 1

def testEachRangePartition(ratingstablename, n, openconnection, rangepartitiontableprefix):
    countList = getCountrangepartition(ratingstablename, n, openconnection)
    cur = openconnection.cursor()
    for i in range(0, n):
        cur.execute("SELECT COUNT(*) FROM `{}`".format(rangepartitiontableprefix + str(i)))
        count = int(cur.fetchone()[0])
        if count != countList[i]:
            raise Exception("{} has {} rows, expected {}".format(rangepartitiontableprefix + str(i), count, countList[i]))

def testEachRoundrobinPartition(ratingstablename, n, openconnection, roundrobinpartitiontableprefix):
    countList = getCountroundrobinpartition(ratingstablename, n, openconnection)
    cur = openconnection.cursor()
    for i in range(0, n):
        cur.execute("SELECT COUNT(*) FROM `{}`".format(roundrobinpartitiontableprefix + str(i)))
        count = int(cur.fetchone()[0])
        if count != countList[i]:
            raise Exception("{} has {} rows, expected {}".format(roundrobinpartitiontableprefix + str(i), count, countList[i]))

def testloadratings(MyAssignment, ratingstablename, filepath, openconnection, rowsininpfile):
    try:
        MyAssignment.loadratings(ratingstablename, filepath, openconnection)
        with openconnection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM `{}`".format(ratingstablename))
            count = int(cur.fetchone()[0])
            if count != rowsininpfile:
                raise Exception("Expected {} rows, found {}".format(rowsininpfile, count))
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]

def testrangepartition(MyAssignment, ratingstablename, n, openconnection, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    try:
        MyAssignment.rangepartition(ratingstablename, n, openconnection)
        testrangeandrobinpartitioning(n, openconnection, RANGE_TABLE_PREFIX, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE)
        testEachRangePartition(ratingstablename, n, openconnection, RANGE_TABLE_PREFIX)
        return [True, None]
    except Exception as e:
        traceback.print_exc()
        return [False, e]
def testrangeinsert(MyAssignment, ratingstablename, userid, itemid, rating, openconnection, expectedtableindex):
    try:
        expectedtablename = RANGE_TABLE_PREFIX + expectedtableindex
        MyAssignment.rangeinsert(ratingstablename, userid, itemid, rating, openconnection)
        if not testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
            raise Exception(
                'Range insert failed! Could not find (%s, %s, %s) tuple in %s table' % (userid, itemid, rating, expectedtablename))
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]

def testroundrobininsert(MyAssignment, ratingstablename, userid, itemid, rating, openconnection, expectedtableindex):
    try:
        expectedtablename = RROBIN_TABLE_PREFIX + expectedtableindex
        MyAssignment.roundrobininsert(ratingstablename, userid, itemid, rating, openconnection)
        if not testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
            raise Exception(
                'Round robin insert failed! Could not find (%s, %s, %s) tuple in %s table' % (userid, itemid, rating, expectedtablename))
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]
def testroundrobinpartition(MyAssignment, ratingstablename, numberofpartitions, openconnection,
                            partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    try:
        MyAssignment.roundrobinpartition(ratingstablename, numberofpartitions, openconnection)
        testrangeandrobinpartitioning(numberofpartitions, openconnection, RROBIN_TABLE_PREFIX, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE)
        testEachRoundrobinPartition(ratingstablename, numberofpartitions, openconnection, RROBIN_TABLE_PREFIX)
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]