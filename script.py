import duckdb
import time
import threading

from datetime import datetime


stop = False

def size(conn):
    while stop == False:
        ## query from another table
        row_count = conn.execute("SELECT COUNT(*) FROM another_table")
        print(row_count.fetchdf())
        time.sleep(10)

if __name__=="__main__":
    conn = duckdb.connect("mydb.db")

    # create table
    now = datetime.now()
    conn.sql("CREATE OR REPLACE TABLE test_table AS select * FROM read_parquet('data/yellow_tripdata_*.parquet')")
    print("completed insert, took " + str((datetime.now() - now).total_seconds()) + " seconds")

    # create another table
    dupconn = conn.duplicate()
    dupconn.sql("CREATE OR REPLACE TABLE another_table AS select * FROM read_parquet('data/yellow_tripdata_*.parquet')")
    dupconn.close()

    t1 = threading.Thread(target=size, args=(conn.duplicate(),))
    t1.start()

    # insert data
    for i in range(5):
        now = datetime.now()
        conn.sql("INSERT INTO test_table select * FROM read_parquet('data/yellow_tripdata_*.parquet')")
        print("completed iteration, took " + str((datetime.now() - now).total_seconds()) + " seconds")
    stop = True
    t1.join()