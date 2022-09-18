import sqlite3
from sqlite3 import Error
import os

class queryDatabase:
    def __init__(self, db_file):
        if not os.path.isfile(db_file):
            print(f"{db_file} is not a file.")
            self.db_file = ""
        else:
            self.db_file = db_file

    # create database connection
    def create_connection(self):
        """ create a database connection to a SQLite database """
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            #print(f"SQLite version: {sqlite3.version}")
        except Error as e:
            print(e)
        return conn

    def execute_query(self, query_str):
        conn = self.create_connection()
        try:
            c = conn.cursor()
            c.execute(query_str)
        except Error as e:
            print(e)
        results = c.fetchall()
        return results

if __name__ == "__main__":
    print("You should not run this script by itself. It should be called from iGDB.py")
    f_name = "../database/db_test.db"
    query = """SELECT *
            FROM ases
            WHERE asn = 3"""

    if not os.path.isfile(f_name):
        print(f"{f_name} is not a file.")
    else:
        my_querier = queryDatabase(f_name)
        my_results = my_querier.execute_query(query)
        print(my_results)

