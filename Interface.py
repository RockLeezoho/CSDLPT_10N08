import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='123456', dbname='postgres'):
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        host='localhost',
        password=password,
        port=5432,
    )

def create_db(dbname):
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=%s', (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % dbname)
    else:
        print(f"A database named {dbname} already exists")

    cur.close()
    con.close()


def create_metadata_table(openconnection):
    """
    Tạo bảng partition_metadata nếu chưa tồn tại
    """
    cur = openconnection.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS partition_metadata (
            partition_type VARCHAR(20) PRIMARY KEY,  -- 'range' hoặc 'roundrobin'
            number_of_partitions INTEGER NOT NULL,
            last_rr_index INTEGER DEFAULT -1  -- chỉ dùng cho roundrobin
        )
    """)
    openconnection.commit()
    cur.close()

def create_roundrobin_partition_metadata_table(openconnection):
    cur = openconnection.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS roundrobin_metadata (
            partition_id SERIAL PRIMARY KEY,
            partition_table_name VARCHAR(50) NOT NULL UNIQUE
        );
    """)
    openconnection.commit()
    cur.close()


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    create_db(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {ratingstablename} CASCADE;")
    cur.execute(f"CREATE TABLE {ratingstablename} (userid integer, extra1 char, movieid integer, extra2 char, rating float, extra3 char, timestamp bigint);")
    with open(ratingsfilepath, 'r') as f:
        cur.copy_from(f, ratingstablename, sep=':')
    cur.execute(f"ALTER TABLE {ratingstablename} DROP COLUMN extra1, DROP COLUMN extra2, DROP COLUMN extra3, DROP COLUMN timestamp;")
    con.commit()
    cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    # Insert vào bảng chính
    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))

    # Dùng lại hàm count_partitions để lấy số phân vùng roundrobin
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    if numberofpartitions == 0:
        cur.close()
        con.commit()
        raise Exception("No roundrobin partitions found in metadata")

    # Lấy last_rr_index từ bảng rr_index_tracker
    cur.execute("SELECT last_rr_index FROM rr_index_tracker WHERE id = 1")
    row = cur.fetchone()
    if row is None:
        last_rr_index = -1
        # Nếu chưa có dòng nào trong tracker, khởi tạo
        cur.execute("INSERT INTO rr_index_tracker (id, last_rr_index) VALUES (1, %s)", (last_rr_index,))
    else:
        last_rr_index = row[0]

    # Tính index phân vùng để insert
    index = (last_rr_index + 1) % numberofpartitions

    # Tên bảng phân vùng tương ứng
    table_name = RROBIN_TABLE_PREFIX + str(index)

    # Insert vào bảng phân vùng tương ứng
    cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))

    # Cập nhật lại last_rr_index trong rr_index_tracker
    cur.execute("UPDATE rr_index_tracker SET last_rr_index = %s WHERE id = 1", (index,))

    con.commit()
    cur.close()



def create_range_partition_metadata_table(openconnection):
    cur = openconnection.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS range_metadata (
            partition_id SERIAL PRIMARY KEY,
            partition_table_name VARCHAR(50) NOT NULL UNIQUE,
            range_start FLOAT NOT NULL,
            range_end FLOAT NOT NULL
        );
    """)
    openconnection.commit()
    cur.close()

def insert_range_partition_metadata(openconnection, partition_table_name, range_start, range_end):
    cur = openconnection.cursor()
    cur.execute("""
        INSERT INTO range_metadata (partition_table_name, range_start, range_end)
        VALUES (%s, %s, %s)
    """, (partition_table_name, range_start, range_end))
    openconnection.commit()
    cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Phân mảnh bảng ratings theo range rating và cập nhật bảng metadata tương ứng.
    """
    con = openconnection
    cur = con.cursor()
    delta = 5.0 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'

    # Tạo bảng metadata nếu chưa có
    create_range_partition_metadata_table(openconnection)

    # Xóa dữ liệu cũ trong metadata để tránh trùng
    cur.execute("DELETE FROM range_metadata;")

    for i in range(numberofpartitions):
        minRange = round(i * delta, 6)
        maxRange = round(minRange + delta, 6)
        table_name = f"{RANGE_TABLE_PREFIX}{i}"

        # Tạo bảng nếu chưa có
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
        """)

        # Chèn dữ liệu vào bảng phân mảnh
        if i == 0:
            cur.execute(
                f"""INSERT INTO {table_name} (userid, movieid, rating)
                    SELECT userid, movieid, rating FROM {ratingstablename}
                    WHERE rating >= %s AND rating <= %s;""",
                (minRange, maxRange)
            )
        else:
            cur.execute(
                f"""INSERT INTO {table_name} (userid, movieid, rating)
                    SELECT userid, movieid, rating FROM {ratingstablename}
                    WHERE rating > %s AND rating <= %s;""",
                (minRange, maxRange)
            )

        # Ghi metadata
        cur.execute("""
            INSERT INTO range_metadata (partition_table_name, range_start, range_end)
            VALUES (%s, %s, %s);
        """, (table_name, minRange, maxRange))

    con.commit()
    cur.close()

    # Kiểm tra số bảng phân mảnh

def create_roundrobin_partition_metadata_table(openconnection):
    cur = openconnection.cursor()
    
    # Tạo bảng metadata lưu thông tin các bảng phân vùng round robin
    cur.execute("""
        CREATE TABLE IF NOT EXISTS roundrobin_metadata (
            partition_id SERIAL PRIMARY KEY,
            partition_table_name VARCHAR(50) NOT NULL UNIQUE
        );
    """)
    
    # Tạo bảng theo dõi chỉ số phân vùng cuối cùng đã insert (round robin index tracker)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rr_index_tracker (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_rr_index INTEGER
        );
    """)
    
    # Đảm bảo có giá trị mặc định ban đầu trong tracker
    cur.execute("""
        INSERT INTO rr_index_tracker (id, last_rr_index)
        VALUES (1, -1)
        ON CONFLICT (id) DO NOTHING;
    """)

    openconnection.commit()
    cur.close()



def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    # Tạo bảng metadata nếu chưa có
    create_roundrobin_partition_metadata_table(openconnection)

    # Xoá metadata cũ
    cur.execute("DELETE FROM roundrobin_metadata;")

    # Xoá các bảng phân vùng cũ nếu có
    for i in range(numberofpartitions):
        cur.execute(f"DROP TABLE IF EXISTS {RROBIN_TABLE_PREFIX}{i} CASCADE;")

    # Tạo lại các bảng phân vùng mới và ghi metadata
    for i in range(numberofpartitions):
        table_name = f"{RROBIN_TABLE_PREFIX}{i}"
        cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT);")

        cur.execute("""
            INSERT INTO {0} (userid, movieid, rating)
            SELECT userid, movieid, rating FROM (
                SELECT userid, movieid, rating, ROW_NUMBER() OVER () as rnum FROM {1}
            ) AS temp
            WHERE MOD(temp.rnum - 1, %s) = %s
        """.format(table_name, ratingstablename), (numberofpartitions, i))

        # Ghi vào metadata
        cur.execute("INSERT INTO roundrobin_metadata (partition_table_name) VALUES (%s);", (table_name,))

    con.commit()
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'

    # Lấy số phân vùng từ metadata
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    if numberofpartitions == 0:
        cur.close()
        raise Exception("No range partitions found in metadata")

    delta = 5 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index -= 1
    if index >= numberofpartitions:
        index = numberofpartitions - 1  # Nếu rating == 5, cho vào phân vùng cuối

    table_name = RANGE_TABLE_PREFIX + str(index)

    # Insert vào bảng chính
    cur.execute(
        f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);",
        (userid, itemid, rating)
    )

    # Insert vào bảng phân vùng
    cur.execute(
        f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s);",
        (userid, itemid, rating)
    )

    con.commit()
    cur.close()

def count_partitions(prefix, openconnection):
    cur = openconnection.cursor()

    if prefix == 'range_part':
        # Đếm số phân vùng trong bảng range_partition_metadata
        cur.execute("SELECT COUNT(*) FROM range_metadata;")
    elif prefix == 'rrobin_part':
        # Đếm số phân vùng trong bảng roundrobin_partition_metadata
        cur.execute("SELECT COUNT(*) FROM roundrobin_partition_metadata;")
    else:
        cur.close()
        return 0

    row = cur.fetchone()
    cur.close()
    return row[0] if row else 0 