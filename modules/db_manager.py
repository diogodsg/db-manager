import os

from colorama import Fore, Style
from modules.query_executor import QueryExecutor
import shutil


class DatabaseManager:
    def execute_query(self, directory: str, query: str):
        QueryExecutor(directory, query).execute()

    def list_tables(self, directory: str):
        print("\nList of Tables:")
        print("{:<5} {:<20}".format("No.", "Table Name"))
        print("-" * 30)

        count = 1
        for table in os.listdir(directory):
            if ".csv" in table:
                table_name = table.replace(".csv", "")
                print("{:<5} {:<20}".format(count, table_name))
                count += 1

    def import_from_csv(self, directory: str):
        file_path = input(
            f"{Fore.YELLOW}Digite o caminho do arquivo: {Style.RESET_ALL}"
        ).strip()

        try:
            shutil.move(file_path, directory)
            print(f"File moved successfully from {file_path} to {directory}")
        except Exception as e:
            print(f"Error: {e}")
