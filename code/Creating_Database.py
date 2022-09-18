import dbStructure as db
import sqlite3
from sqlite3 import Error
from pathlib import Path
import os
import csv

class CreatingDatabase:
    """This class is called by iGDB.py to create a new database
    using the format described in dbStructure.py and
    load data into each table from processed files."""
    def __init__(self, in_path, out_path, f_name):
        if not os.path.isdir(out_path):
            os.makedirs(out_path)
        db_file = out_path / f_name
        if os.path.isfile(db_file):
            os.remove(db_file)
        print(f"Creating DB here: {db_file}")
        self.input_path = in_path
        db_conn = self.create_connection(db_file)
        # create the tables and add the data
        for t in db.tables.keys():
            #print(f"Creating {t}")
            self.create_table(db_conn, db.tables[t])
            #input("Done with creation. ENTER to continue.")
            self.load_table(db_conn, t)

    def create_connection(self, db_file):
        """ create a database connection to a SQLite database """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
            print(f"SQLite version: {sqlite3.version}")
            #conn = spatialite.connect(db_file)
            #print(f"SpatiaLite version: {spatialite.version}")
        except Error as e:
            print(e)
        return conn

    def create_table(self, conn, create_table_sql):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        try:
            c = conn.cursor()
            c.execute(create_table_sql)
        except Error as e:
            print(e)

    def load_table(self, conn, table_type):
        """This is a more general version of the loading function.
        It assumes that the columns of the input csv file are the same as the
        attributes in the table we are inserting into.
        "table_type" should be the name of a table in the DB."""
        cur = conn.cursor()
        local_path = self.input_path / table_type
        if not os.path.isdir(local_path):
            print(f"No existing data of type {table_type}.")
            return

        for f in os.listdir(local_path):
            print(f"Loading data from: {f}")
            sql_base = f"INSERT INTO {table_type}("
            with open(local_path / f, 'r') as f:
                csv_reader = csv.reader(f, delimiter=',')
                # read in the header and add all the header values to the SQL query
                # the header values must correspond to attribute fields in the DB
                header = next(csv_reader)
                for v in header:
                    sql_base += f"{v},"
                sql_base = sql_base[:-1]
                sql_base += ") VALUES("

                for row in csv_reader:
                    sql = sql_base
                    for r in row:
                        sql += f"'{r}',"
                    sql = sql[:-1]
                    sql += ")"
                    try:
                        cur.execute(sql)
                    except Error as e:
                        print(f"{e}: {row}")
        conn.commit()

if __name__ == "__main__":
    print("You should not run this script by itself. It should be called from iGDB.py")
    input_path = Path("../processed")
    output_path = Path("../database")
    my_db = CreatingDatabase(input_path, output_path, "test_database.db")
