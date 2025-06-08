#!/usr/bin/python2.7
#
# Interface for the assignment
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='123sql', dbname='postgres'):
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        host='localhost',
        password=password
    )


import psycopg2

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Load ratings data from file into the specified table in the database.
    Only keeps columns: UserID (int), MovieID (int), Rating (float)
    """
    try:
        cur = openconnection.cursor()

        # Tạo bảng nếu chưa tồn tại
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {ratingstablename} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
        """
        cur.execute(create_table_query)

        # Đọc và chèn dữ liệu
        with open(ratingsfilepath, 'r') as f:
            for line in f:
                try:
                    parts = line.strip().split("::")
                    if len(parts) != 4:
                        continue  # Bỏ qua dòng sai định dạng
                    userid = int(parts[0])
                    movieid = int(parts[1])
                    rating = float(parts[2])
                    # Không lưu timestamp theo yêu cầu đề bài
                    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);",
                                (userid, movieid, rating))
                except Exception as inner_err:
                    print(f"Lỗi dòng: {line.strip()} - {inner_err}")
                    continue

        openconnection.commit()
        cur.close()

    except Exception as e:
        print(f"Lỗi khi tải dữ liệu: {e}")


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Partition the ratingstablename into numberofpartitions partitions by rating range.
    """
    con = openconnection
    cur = con.cursor()

    try:
        delta = 5.0 / numberofpartitions

        # Tạo bảng meta lưu số phân vùng
        cur.execute("DROP TABLE IF EXISTS range_meta")
        cur.execute("CREATE TABLE range_meta (numberofpartitions INT)")
        cur.execute("INSERT INTO range_meta VALUES (%s)", (numberofpartitions,))

        # Tạo từng bảng phân vùng
        for i in range(numberofpartitions):
            lower = i * delta
            upper = (i + 1) * delta
            part_table = f"range_part{i}"

            cur.execute(f"DROP TABLE IF EXISTS {part_table}")

            if i == 0:
                # Phân vùng đầu: rating >= lower AND rating <= upper
                cur.execute(f"""
                    CREATE TABLE {part_table} AS
                    SELECT userid, movieid, rating FROM {ratingstablename}
                    WHERE rating >= %s AND rating <= %s;
                """, (lower, upper))
            else:
                # Các phân vùng còn lại: rating > lower AND rating <= upper
                cur.execute(f"""
                    CREATE TABLE {part_table} AS
                    SELECT userid, movieid, rating FROM {ratingstablename}
                    WHERE rating > %s AND rating <= %s;
                """, (lower, upper))

        con.commit()

    except Exception as e:
        con.rollback()
        print(f"[ERROR] rangepartition failed: {e}")

    finally:
        cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Partition table using round-robin strategy.
    """
    con = openconnection
    cur = con.cursor()

    try:
        # Xoá bảng meta cũ nếu có
        cur.execute("DROP TABLE IF EXISTS rrobin_meta;")
        cur.execute("""
            CREATE TABLE rrobin_meta (
                partition_count INT,
                last_inserted INT
            );
        """)
        cur.execute("INSERT INTO rrobin_meta (partition_count, last_inserted) VALUES (%s, %s);",
                    (numberofpartitions, -1))

        # Tạo các bảng phân vùng round robin
        for part in range(numberofpartitions):
            part_table = f"rrobin_part{part}"
            cur.execute(f"DROP TABLE IF EXISTS {part_table};")

            # Tạo bảng phân vùng, nhúng trực tiếp giá trị numberofpartitions và part
            cur.execute(f"""
                CREATE TABLE {part_table} AS
                SELECT userid, movieid, rating
                FROM (
                    SELECT userid, movieid, rating, ROW_NUMBER() OVER () as row_num
                    FROM {ratingstablename}
                ) AS numbered
                WHERE (row_num - 1) % {numberofpartitions} = {part};
            """)

        con.commit()

    except Exception as e:
        con.rollback()
        print(f"[ERROR] roundrobinpartition failed: {e}")

    finally:
        cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Insert a single row into the main ratings table and the appropriate round robin partition.
    This function relies on rr_metadata table created in roundrobinpartition.
    """
    cur = openconnection.cursor()
    try:
        # 1. Lấy số phân mảnh và vị trí chèn cuối cùng từ rrobin_meta
        cur.execute("SELECT partition_count, last_inserted FROM rrobin_meta;")
        result = cur.fetchone()
        if not result:
            raise Exception("rrobin_meta chưa được khởi tạo. Hãy gọi roundrobinpartition trước.")

        partition_count, last_inserted = result
        next_partition = (last_inserted + 1) % partition_count

        # 2. Chèn vào bảng chính
        cur.execute(f"""
            INSERT INTO {ratingstablename} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))

        # 3. Chèn vào bảng phân mảnh đúng
        cur.execute(f"""
            INSERT INTO rrobin_part{next_partition} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))

        # 4. Cập nhật metadata
        cur.execute("UPDATE rrobin_meta SET last_inserted = %s;", (next_partition,))
        openconnection.commit()

    except Exception as e:
        print(f"Error in roundrobininsert: {e}")
        openconnection.rollback()
    finally:
        cur.close() 

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    RANGE_TABLE_PREFIX = 'range_part'

    try:
        # Chèn vào bảng chính
        cur.execute(f"""
            INSERT INTO {ratingstablename} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))

        # Lấy số phân vùng từ bảng range_meta
        cur.execute("SELECT numberofpartitions FROM range_meta")
        row = cur.fetchone()
        if not row:
            raise Exception("range_meta chưa được khởi tạo. Hãy gọi rangepartition trước.")
        numberofpartitions = row[0]

        delta = 5.0 / numberofpartitions

        # Hàm xác định phân vùng theo rating (giống rangepartition)
        def get_partition_index(r, delta, numberofpartitions):
            if r == 0:
                return 0
            for i in range(numberofpartitions):
                min_bound = i * delta
                max_bound = (i + 1) * delta
                if i == 0:
                    if r >= min_bound and r <= max_bound:
                        return i
                else:
                    if r > min_bound and r <= max_bound:
                        return i
            return numberofpartitions - 1

        index = get_partition_index(rating, delta, numberofpartitions)
        partition_table = f"{RANGE_TABLE_PREFIX}{index}"

        # Chèn vào bảng phân vùng tương ứng
        cur.execute(f"""
            INSERT INTO {partition_table} (userid, movieid, rating)
            VALUES (%s, %s, %s);
        """, (userid, itemid, rating))

        openconnection.commit()

    except Exception as e:
        openconnection.rollback()
        print(f"[ERROR] rangeinsert failed: {e}")

    finally:
        cur.close()
