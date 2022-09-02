import psycopg2
import logging
import os

RECORD_SIZE = 10000
NO_OF_TIMES = 2

def main(request):
    try:
        write_db_conn = get_write_db_connection()
        record_size = RECORD_SIZE
        update_query = query_for_updating_records(record_size)
        for x in range(NO_OF_TIMES):
            run_query(update_query, write_db_conn)

        write_db_conn.close()
    except Exception as e:
        logging.exception("exception in main function: {}".format(e))


def query_for_updating_records(record_size):
    query_prefix = 'UPDATE transactions_transaction SET status = \'failed\', failed_by = \'CRON\' WHERE id IN ( '
    query_suffix_1 = 'SELECT id FROM transactions_transaction WHERE ( '
    query_suffix_2 = 'status IN (\'initiated\', \'checksum_verified\', \'generated_checksum\' ) '
    query_suffix_3 = 'AND timezone(\'Asia/Kolkata\', created_on)::DATE < \'2022-08-20\' AND type <> \'REFUND\''
    query_suffix_4 = ' ) LIMIT {}'.format(record_size)
    query_end = " );"
    query = query_prefix + query_suffix_1 + query_suffix_2 + query_suffix_3 + query_suffix_4 + query_end
    return query


def run_query(update_query, write_db_conn):
    try:
        cursor = write_db_conn.cursor()
        cursor.execute(update_query)
        write_db_conn.commit()
    except Exception as e:
        write_db_conn.rollback()
        logging.exception("Exception while committing results: {}".format(e))


def get_write_db_connection():
    espresso_db_password = os.environ['espresso-write-db-pwd']
    conn = psycopg2.connect(database="espresso", user="prod_vpc_espresso_primary_user_1",
                             password=espresso_db_password,
                             host="10.14.16.43",
                             port="6432")
    return conn

