#
# Tester for the assignement1
#

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = 'test_data.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 20  # Number of lines in the input file

# import mysql.connector  # Removed for PostgreSQL compatibility
import traceback
import testHelper
import Interface as MyAssignment

if __name__ == '__main__':
    try:
        # Nhập tên database từ người dùng
        DATABASE_NAME = input("Nhập tên database: ")
        
        # Xóa database cũ nếu tồn tại
        testHelper.delete_db(DATABASE_NAME)
        # Tạo database mới
        testHelper.createdb(DATABASE_NAME)

        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            testHelper.deleteAllPublicTables(conn)

            # Load dữ liệu ban đầu
            [result, e] = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
            if result:
                print("loadratings function pass!")
            else:
                print("loadratings function fail!")

            # Thực hiện phân mảnh theo Range
            print("\n=== Bắt đầu phân mảnh theo Range ===")
            while True:
                try:
                    num_range_partitions = int(input("Nhập số phân mảnh Range cần tạo: "))
                    if num_range_partitions > 0:
                        break
                    else:
                        print("Số phân mảnh phải lớn hơn 0!")
                except ValueError:
                    print("Vui lòng nhập một số nguyên!")

            [result, e] = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, num_range_partitions, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            if result:
                print("rangepartition function pass!")
            else:
                print("rangepartition function fail!")
            print("=== Hoàn thành phân mảnh theo Range ===\n")


            # Thực hiện phân mảnh theo Round Robin TRƯỚC khi test rangeinsert
            print("\n=== Bắt đầu phân mảnh theo Round Robin ===")
            while True:
                try:
                    num_robin_partitions = int(input("Nhập số phân mảnh Round Robin cần tạo: "))
                    if num_robin_partitions > 0:
                        break
                    else:
                        print("Số phân mảnh phải lớn hơn 0!")
                except ValueError:
                    print("Vui lòng nhập một số nguyên!")

            [result, e] = testHelper.testroundrobinpartition(MyAssignment, RATINGS_TABLE, num_robin_partitions, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            if result:
                print("roundrobinpartition function pass!")
            else:
                print("roundrobinpartition function fail")

            # Test round robin insert
            [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '0')
            if result:
                print("roundrobininsert function pass!")
            else:
                print("roundrobininsert function fail!")

            # Test rangeinsert với giá trị rating = 3.0
            print("\n=== Test Range Insert ===")
            test_userid = 100
            test_movieid = 2
            test_rating = 3.0
            
            # Gọi hàm rangeinsert
            MyAssignment.rangeinsert(RATINGS_TABLE, test_userid, test_movieid, test_rating, conn)
            
            # Kiểm tra kết quả
            cur = conn.cursor()
            
            # Kiểm tra trong bảng chính
            cur.execute(f"""
                SELECT COUNT(*) 
                FROM {RATINGS_TABLE} 
                WHERE {USER_ID_COLNAME} = {test_userid} 
                AND {MOVIE_ID_COLNAME} = {test_movieid} 
                AND {RATING_COLNAME} = {test_rating}
            """)
            main_count = cur.fetchone()[0]
            
            # Kiểm tra trong phân mảnh
            cur.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_name LIKE 'range_part%'
            """)
            num_partitions = cur.fetchone()[0]
            
            if num_partitions > 0:
                delta = 5.0 / num_partitions
                partition_index = int(test_rating / delta)
                if test_rating % delta == 0 and partition_index != 0:
                    partition_index = partition_index - 1
                    
                partition_name = f'range_part{partition_index}'
                cur.execute(f"""
                    SELECT COUNT(*) 
                    FROM {partition_name} 
                    WHERE {USER_ID_COLNAME} = {test_userid} 
                    AND {MOVIE_ID_COLNAME} = {test_movieid} 
                    AND {RATING_COLNAME} = {test_rating}
                """)
                partition_count = cur.fetchone()[0]
                
                if main_count == 1 and partition_count == 1:
                    print("rangeinsert function pass!")
                else:
                    print("rangeinsert function fail!")
                    print(f"Main table count: {main_count}")
                    print(f"Partition count: {partition_count}")
            else:
                print("No partitions found!")
            
            # Lấy số hàng hiện tại sau khi thực hiện rangeinsert
            cur.execute(f"SELECT COUNT(*) FROM {RATINGS_TABLE}")
            current_row_count = cur.fetchone()[0]
            print(f"\nSố hàng hiện tại trong bảng {RATINGS_TABLE}: {current_row_count}")
            
            cur.close()

            choice = input('Press enter to Delete all tables? ')
            if choice == '':
                testHelper.deleteAllPublicTables(conn)
            if not conn.close:
                conn.close()

    except Exception as detail:
        traceback.print_exc()