import os
from db_manager import DatabaseManager
from db_connection import DatabaseConnection


# Main function for CLI
class DatabaseCli:
    def __init__(self) -> None:
        self.directory = "./database"
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
            print(f"Directory '{self.directory}' created successfully.")

        self.db_manager = DatabaseManager()
        self.db_connection = DatabaseConnection(self.directory)
        print(self.db_manager)

    def main(self):
        print("Welcome to the Database Manager CLI!")
        print(f"Your database directory is set to be {self.directory}!")
        while True:
            print("\nOptions:")
            print("1. executar query")
            print("2. importar do MySql")

            choice = input("Enter your choice (1-5): ")

            if choice == "1":
                query = input("Enter your query:\n> ").strip()
                while query[-1] != ";":
                    query_part = input("> ").strip()
                    query += " " + query_part
                query = query.replace(";", "")
                self.db_manager.execute_query(self.directory, query)
            elif choice == "2":
                self.db_connection.import_from_database()

            else:
                print("Invalid choice. Please enter a number between 1 and 5.")


if __name__ == "__main__":
    cli = DatabaseCli()
    cli.main()
