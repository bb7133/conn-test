import mysql.connector
from threading import Thread
import time
import random


HOST = '127.0.0.1'
PORT = '4000'
USER = 'root'
PASSWORD = ''
DATABASE = 'test'

NUM_TABLES = 10
SIZE_TABLE = 100000


def point_get(cursor):
    random_id = random.randint(0, SIZE_TABLE - 1)
    random_table = random.randint(1, NUM_TABLES)
    query = f"SELECT * FROM sbtest{random_table} WHERE id = {random_id}"
    cursor.execute(query)
    result = cursor.fetchall()
    return

def simple_range(cursor):
    random_low = random.randint(0, SIZE_TABLE - 1)
    random_up = random.randint(random_low, SIZE_TABLE - 1)
    random_table = random.randint(1, NUM_TABLES)
    query = f"SELECT c FROM sbtest{random_table} WHERE id between {random_low} and {random_up}"
    cursor.execute(query)
    result = cursor.fetchall()
    return

def sum_ranges(cursor):
    random_low = random.randint(0, SIZE_TABLE - 1)
    random_up = random.randint(random_low, SIZE_TABLE - 1)
    random_table = random.randint(1, NUM_TABLES)
    query = f"SELECT SUM(k) FROM sbtest{random_table} WHERE id BETWEEN {random_low} AND {random_up}"
    cursor.execute(query)
    result = cursor.fetchall()
    return

def order_ranges(cursor):
    random_low = random.randint(0, SIZE_TABLE - 1)
    random_up = random.randint(random_low, SIZE_TABLE - 1)
    random_table = random.randint(1, NUM_TABLES)
    query = f"SELECT c FROM sbtest{random_table} WHERE id BETWEEN {random_low} AND {random_up} ORDER BY c"
    cursor.execute(query)
    result = cursor.fetchall()
    return

def distinct_ranges(cursor):
    random_low = random.randint(0, SIZE_TABLE - 1)
    random_up = random.randint(random_low, SIZE_TABLE - 1)
    random_table = random.randint(1, NUM_TABLES)
    query = f"SELECT DISTINCT c FROM sbtest{random_table} WHERE id BETWEEN {random_low} AND {random_up} ORDER BY c"
    cursor.execute(query)
    result = cursor.fetchall()
    return


### Sysbench Write Pattern
# non_index_updates = "UPDATE sbtest%u SET c=? WHERE id=?"
# deletes = "DELETE FROM sbtest%u WHERE id=?"
###

SQLS = [point_get]


# Function to execute queries
def execute_queries(thread_id):
    conn = mysql.connector.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE
    )

    # Connect to MySQL database
    cursor = conn.cursor()

    interval = 5 # in sec

    # Execute sysbench-like queries
    start_time = time.time()
    try:
        while True:
            time.sleep(interval)
            random.choice(SQLS)(cursor)
    except Exception as e:
        print(e)

    end_time = time.time()

    # Print execution time for each thread
    print(f"Thread {thread_id} finished in {end_time - start_time} seconds")

    # Conn not closed
    cursor.close()
    return


# Function for idle connection
def idle(thread_id):
    conn = mysql.connector.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE
    )

    # Execute sysbench-like queries
    start_time = time.time()
    try:
        while True:
            time.sleep(500)
    except Exception as e:
        print(e)

    end_time = time.time()

    # Print execution time for each thread
    print(f"Thread {thread_id} finished in {end_time - start_time} seconds")
    return


if __name__ == "__main__":
    # Number of threads (connections)
    tasks = [idle] * 100 + [execute_queries] * 10

    # Start threads
    threads = []
    for i, t in enumerate(tasks):
        thread = Thread(target=t, args=(i,))
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        print("Done")
        import sys
        sys.exit(0)

