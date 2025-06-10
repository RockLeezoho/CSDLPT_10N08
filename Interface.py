import psycopg2
import psycopg2.extensions
import time

def getopenconnection(user='postgres', password='123456', dbname='postgres'):
    try:
        return psycopg2.connect(
            dbname=dbname,
            user=user,
            host='localhost',
            password=password,
            port=5432,
        )
    except Exception as e:
        print(f"Error connecting to DB: {e}")
        raise

def create_db(dbname, user='postgres', password='123456'):
    con = None
    cur = None
    try:
        con = getopenconnection(user=user, password=password, dbname='postgres')
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=%s', (dbname,))
        count = cur.fetchone()[0]
        if count == 0:
            cur.execute(f'CREATE DATABASE {dbname}')
            print(f"Database {dbname} created.")
        else:
            print(f"Database {dbname} already exists.")
    except Exception as e:
        print(f"Error creating database: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if con:
            con.close()

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    start = time.time()
    cur = None
    try:
        cur = openconnection.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {ratingstablename} CASCADE;")
        cur.execute(f"""
            CREATE TABLE {ratingstablename} (
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
        openconnection.commit()
        print(f"[TIME]Data loaded into {ratingstablename} in {time.time() - start:.2f} seconds.")
    except Exception as e:
        if openconnection:
            openconnection.rollback()
        print(f"Error loading ratings: {e}")
        raise
    finally:
        if cur:
            cur.close()

def create_range_partition_metadata_table(openconnection):
    cur = None
    try:
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
    except Exception as e:
        print(f"Error creating range_metadata table: {e}")
        raise
    finally:
        if cur:
            cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    import time
    start = time.time()
    cur = None
    try:
        if numberofpartitions <= 0:
            raise ValueError("Number of partitions must be greater than 0.")

        cur = openconnection.cursor()
        create_range_partition_metadata_table(openconnection)

        cur.execute("DELETE FROM range_metadata;")
        for i in range(numberofpartitions):
            cur.execute(f"DROP TABLE IF EXISTS range_part{i} CASCADE;")

        delta = 5.0 / numberofpartitions

        for i in range(numberofpartitions):
            minRange = round(i * delta, 6)
            maxRange = round(minRange + delta, 6)
            table_name = f"range_part{i}"

            cur.execute(f"""
                CREATE TABLE {table_name} (
                    userid INTEGER,
                    movieid INTEGER,
                    rating FLOAT
                );
            """)

            if i == 0:
                cur.execute(f"""
                    INSERT INTO {table_name} (userid, movieid, rating)
                    SELECT userid, movieid, rating FROM {ratingstablename}
                    WHERE rating >= %s AND rating <= %s;
                """, (minRange, maxRange))
            else:
                cur.execute(f"""
                    INSERT INTO {table_name} (userid, movieid, rating)
                    SELECT userid, movieid, rating FROM {ratingstablename}
                    WHERE rating > %s AND rating <= %s;
                """, (minRange, maxRange))

            cur.execute("""
                INSERT INTO range_metadata (partition_table_name, range_start, range_end)
                VALUES (%s, %s, %s);
            """, (table_name, minRange, maxRange))

        openconnection.commit()
        print(f"[TIME] Range partition completed in {time.time() - start:.2f} seconds.")
    except Exception as e:
        if openconnection:
            openconnection.rollback()
        print(f"Error in range partition: {e}")
        raise
    finally:
        if cur:
            cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    start = time.time()
    cur = None
    try:
        cur = openconnection.cursor()
        numberofpartitions = count_partitions('range_part', openconnection)
        if numberofpartitions == 0:
            raise Exception("No range partitions found in metadata")
        delta = 5.0 / numberofpartitions
        index = int(rating / delta)
        if rating % delta == 0 and index != 0:
            index -= 1
        if index >= numberofpartitions:
            index = numberofpartitions - 1
        table_name = f"range_part{index}"
        cur.execute(
            f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);",
            (userid, itemid, rating)
        )
        cur.execute(
            f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s);",
            (userid, itemid, rating)
        )
        openconnection.commit()
        print(f"[TIME]Range insert done in {time.time() - start:.4f} seconds.")
    except Exception as e:
        if openconnection:
            openconnection.rollback()
        print(f"Error in rangeinsert: {e}")
        raise
    finally:
        if cur:
            cur.close()

def create_roundrobin_partition_metadata_table(openconnection):
    cur = None
    try:
        cur = openconnection.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS roundrobin_metadata (
                partition_id SERIAL PRIMARY KEY,
                partition_table_name VARCHAR(50) NOT NULL UNIQUE
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rr_index_tracker (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_rr_index INTEGER
            );
        """)
        cur.execute("""
            INSERT INTO rr_index_tracker (id, last_rr_index)
            VALUES (1, -1)
            ON CONFLICT (id) DO NOTHING;
        """)
        openconnection.commit()
    except Exception as e:
        print(f"Error creating roundrobin metadata tables: {e}")
        raise
    finally:
        if cur:
            cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    import time
    start = time.time()
    cur = None
    try:
        cur = openconnection.cursor()

        create_roundrobin_partition_metadata_table(openconnection)
        cur.execute("DELETE FROM roundrobin_metadata;")

        # Drop và tạo lại các partition
        for i in range(numberofpartitions):
            cur.execute(f"DROP TABLE IF EXISTS rrobin_part{i} CASCADE;")
            cur.execute(f"CREATE TABLE rrobin_part{i} (userid INTEGER, movieid INTEGER, rating FLOAT);")

        # Tạo bảng tạm chứa dữ liệu đã đánh số thứ tự
        cur.execute("DROP TABLE IF EXISTS temp_rr_table;")
        cur.execute(f"""
            CREATE TEMP TABLE temp_rr_table AS
            SELECT userid, movieid, rating,
                   ROW_NUMBER() OVER (ORDER BY userid, movieid) AS rnum
            FROM {ratingstablename};
        """)

        # Chèn dữ liệu vào từng partition
        for i in range(numberofpartitions):
            cur.execute(f"""
                INSERT INTO rrobin_part{i} (userid, movieid, rating)
                SELECT userid, movieid, rating
                FROM temp_rr_table
                WHERE MOD(rnum - 1, %s) = %s;
            """, (numberofpartitions, i))
            cur.execute("INSERT INTO roundrobin_metadata (partition_table_name) VALUES (%s);", (f"rrobin_part{i}",))

        # Tính last_rr_index
        cur.execute("SELECT COUNT(*) FROM temp_rr_table;")
        total_rows = cur.fetchone()[0]
        last_index = (total_rows - 1) % numberofpartitions if total_rows > 0 else -1

        # Cập nhật tracker
        cur.execute("""
            INSERT INTO rr_index_tracker (id, last_rr_index)
            VALUES (1, %s)
            ON CONFLICT (id) DO UPDATE SET last_rr_index = EXCLUDED.last_rr_index;
        """, (last_index,))

        openconnection.commit()
        print(f"[TIME] Round robin partition completed in {time.time() - start:.2f} seconds.")
    except Exception as e:
        if openconnection:
            openconnection.rollback()
        print(f"Error in roundrobin partition: {e}")
        raise
    finally:
        if cur:
            cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    start = time.time()
    cur = None
    try:
        cur = openconnection.cursor()
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
        numberofpartitions = count_partitions('rrobin_part', openconnection)
        if numberofpartitions == 0:
            raise Exception("No roundrobin partitions found in metadata")
        cur.execute("SELECT last_rr_index FROM rr_index_tracker WHERE id = 1")
        row = cur.fetchone()
        last_rr_index = row[0] if row else -1
        index = (last_rr_index + 1) % numberofpartitions
        table_name = f"rrobin_part{index}"
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)", (userid, itemid, rating))
        cur.execute("UPDATE rr_index_tracker SET last_rr_index = %s WHERE id = 1", (index,))
        openconnection.commit()
        print(f"[TIME]Round robin insert done in {time.time() - start:.4f} seconds.")
    except Exception as e:
        if openconnection:
            openconnection.rollback()
        print(f"Error in roundrobininsert: {e}")
        raise
    finally:
        if cur:
            cur.close()

def count_partitions(prefix, openconnection):
    cur = None
    try:
        cur = openconnection.cursor()
        if prefix == 'range_part':
            cur.execute("SELECT COUNT(*) FROM range_metadata;")
        elif prefix == 'rrobin_part':
            cur.execute("SELECT COUNT(*) FROM roundrobin_metadata;")
        else:
            return 0
        row = cur.fetchone()
        return row[0] if row else 0
    except Exception as e:
        print(f"Error counting partitions: {e}")
        return 0
    finally:
        if cur:
            cur.close()
