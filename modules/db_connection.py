from colorama import Fore, Style
import mysql.connector
import pandas as pd
from modules.config_manager import ConfigManager
import os


class DatabaseConnection:
    def __init__(self, directory):
        self.directory = directory
        self.create_connection()

    def connect(self):
        print("Vamos conectar com seu MySql")
        host = input("Escreva o host: ")
        port = input("Escreva a port: ")
        user = input("Escreva o usuario: ")
        password = input("Escreva a senha: ")

        credentials = {
            "user": user,
            "password": password,
            "host": host,
            "port": port,
        }

        ConfigManager.save_mysql_config(credentials)
        self.create_connection()

    def _select_database(self, cursor):
        cursor.execute("SHOW DATABASES;")
        databases = cursor.fetchall()

        print("\Lista de Banco de Dados:")
        print("{:<5} {:<20}".format("No.", "Banco de Dados"))
        print("-" * 30)
        for i in range(len(databases)):
            print("{:<5} {:<20}".format(i + 1, databases[i][0]))

        selected_db = int(
            input(f"{Fore.YELLOW}Selecione um banco de dados: {Style.RESET_ALL}")
        )
        cursor.execute(f"use {databases[selected_db-1][0]};")

    def _select_table(self, cursor):
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()

        print("\nLista de Tabelas:")
        print("{:<5} {:<20}".format("No.", "Tabela"))
        print("-" * 30)

        for i in range(len(tables)):
            print("{:<5} {:<20}".format(i, tables[i][0]))
        selected_db = int(
            input(f"{Fore.YELLOW}Selecione uma tabela: {Style.RESET_ALL}")
        )
        return tables[selected_db][0]

    def _write_csv(self, table: str, conn):
        query = f"SELECT * FROM {table};"
        print(query)
        df = pd.read_sql_query(query, conn)
        csv_filename = os.path.join(self.directory, f"{table}.csv")
        df.to_csv(csv_filename, index=False)

    def create_connection(self):
        config = ConfigManager.load_mysql_config()
        try:
            conn = mysql.connector.connect(**config)
            if conn.is_connected():
                print(
                    f"{Fore.GREEN}Você está conectado à um Banco MySQL.({config['host']}: {config['port']}){Style.RESET_ALL}"
                )
                return conn

            print(
                f"{Fore.RED}Credenciais Inválidas ou inexistentes para o MySQL.{Style.RESET_ALL}"
            )
            return False

        except mysql.connector.Error as e:
            print(
                f"{Fore.RED}Credenciais Inválidas ou inexistentes para o MySQL.{Style.RESET_ALL}"
            )
            return False

    def import_from_database(self):
        conn = self.create_connection()
        cursor = conn.cursor()

        self._select_database(cursor)
        table = self._select_table(cursor)
        self._write_csv(table, conn)
        conn.close()
