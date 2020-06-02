import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Staging song data and log data json files from AWS S3 to Redshift
    
    Arguments:
            cur {object}:    Allows Python code to execute PostgreSQL command in a database session
            conn {object}:   Handles the connection to a PostgreSQL database instance. It encapsulates a database session.
    Returns:
            No return values
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Transforming data from staging tables into dimensional tables
    
    Arguments:
            cur {object}:    Allows Python code to execute PostgreSQL command in a database session
            conn {object}:   Handles the connection to a PostgreSQL database instance. It encapsulates a database session.
    Returns:
            No return values
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connecting to a database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()