import sqlite3
from modules.query_executor import QueryExecutor


class DatabaseManager:
    # Function to create a new database
    def execute_query(self, directory: str, query: str):
        QueryExecutor(directory, query).execute()

    # Function to create a new database
    def create_database(self, db_name: str):
        conn = sqlite3.connect(db_name)
        conn.close()
        print(f"Database '{db_name}' created successfully!")

    # Function to create a new table
    def create_table(self, db_name, table_name, columns):
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(f"CREATE TABLE {table_name} ({columns})")
        conn.commit()
        conn.close()
        print(f"Table '{table_name}' created successfully!")

    # Function to insert data into the table
    def insert_data(db_name, table_name, values):
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(f"INSERT INTO {table_name} VALUES ({values})")
        conn.commit()
        conn.close()
        print("Data inserted successfully!")

    # Function to display all data from a table
    def display_data(db_name, table_name):
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        conn.close()

        if len(rows) > 0:
            for row in rows:
                print(row)
        else:
            print("No data found in the table.")
