import configparser
import mysql.connector
import pandas as pd
import os


class DatabaseConnection:
    def __init__(self, directory):
        self.directory = directory

    def connect(self):
        # print("Vamos conectar com seu MySql")
        # host = input("Escreva o host: ")
        # port = input("Escreva o host: ")
        # user = input("Escreva o usuario: ")
        # password = input("Escreva o usuario: ")

        # credentials = {
        #     "user": user,
        #     "password": password,
        #     "host": host,
        #     "port": port,
        # }

        # self.save_credentials_to_ini(credentials)
        self.create_connection()

    def save_credentials_to_ini(self, credentials):
        config = configparser.ConfigParser()
        config["mysql"] = credentials

        with open("mysql_credentials.ini", "w") as configfile:
            config.write(configfile)
        print("Credentials saved to 'mysql_credentials.ini'.")

    def read_credentials_from_ini(self, filename="mysql_credentials.ini"):
        config = configparser.ConfigParser()
        config.read(filename)

        credentials = {
            "user": config.get("mysql", "user"),
            "password": config.get("mysql", "password"),
            "host": config.get("mysql", "host"),
            "port": config.get("mysql", "port"),
        }
        return credentials

    def select_database(self, cursor):
        cursor.execute("SHOW DATABASES;")
        databases = cursor.fetchall()
        for i in range(len(databases)):
            print(f"{i+1}: {databases[i][0]}")

        selected_db = int(input("Selecione um banco de dados"))
        cursor.execute(f"use {databases[selected_db-1][0]};")

    def select_table(self, cursor, conn):
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()
        for i in range(len(tables)):
            print(f"{i+1}: {tables[i][0]}")
        selected_db = int(input("Selecione um banco de dados"))
        table = tables[selected_db - 1][0]
        query = f"SELECT * FROM {table} LIMIT 50;"
        df = pd.read_sql_query(query, conn)

        # Write DataFrame to a CSV file
        csv_filename = os.path.join(self.directory, f"{table}.csv")
        df.to_csv(csv_filename, index=False)
        cursor.execute(f"SELECT * FROM {table} LIMIT 100;")
        rows = cursor.fetchall()
        print(rows)

    def create_connection(self):
        config = self.read_credentials_from_ini()
        try:
            conn = mysql.connector.connect(**config)
            if conn.is_connected():
                return conn

            return False

        except mysql.connector.Error as e:
            return False

    def import_from_database(self):
        conn = self.create_connection()
        cursor = conn.cursor()

        self.select_database(cursor)
        self.select_table(cursor, conn)
        conn.close()
