import os
from colorama import Fore, Style
from modules.db_manager import DatabaseManager
from modules.db_connection import DatabaseConnection


class DatabaseCli:
    def __init__(self) -> None:
        self.directory = "./database"
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
            print(
                f"{Fore.GREEN}Directory '{self.directory}' created successfully.{Style.RESET_ALL}"
            )

        self.db_manager = DatabaseManager()
        self.db_connection = DatabaseConnection(self.directory)

    def display_options(self):
        print("\nOptions:")
        print(f"{Fore.GREEN}Current Directory: {self.directory}{Style.RESET_ALL}")
        print(f"{Fore.CYAN}1. Executar Query{Style.RESET_ALL}")
        print(f"{Fore.CYAN}2. Importar do MySQL{Style.RESET_ALL}")
        print(f"{Fore.CYAN}3. Importar de csv{Style.RESET_ALL}")
        print(f"{Fore.CYAN}4. Conectar com MySQL{Style.RESET_ALL}")
        print(f"{Fore.CYAN}5. Alterar Diretório{Style.RESET_ALL}")

    def main(self):
        print(f"{Fore.YELLOW}Welcome to the Database Manager CLI!{Style.RESET_ALL}")
        print(
            f"Your database directory is set to be {Fore.MAGENTA}{self.directory}{Style.RESET_ALL}!"
        )

        while True:
            self.display_options()
            choice = input(f"{Fore.YELLOW}Enter your choice (1-2): {Style.RESET_ALL}")

            if choice == "1":
                query = input(
                    f"{Fore.YELLOW}Enter your query:\n> {Style.RESET_ALL}"
                ).strip()
                while query[-1] != ";":
                    query_part = input(f"{Fore.YELLOW}> {Style.RESET_ALL}").strip()
                    query += " " + query_part
                query = query.replace(";", "")
                self.db_manager.execute_query(self.directory, query)
            elif choice == "2":
                self.db_connection.import_from_database()
            elif choice == "4":
                self.db_connection.connect()
            else:
                print(
                    f"{Fore.RED}Invalid choice. Please enter a number between 1 and 2.{Style.RESET_ALL}"
                )


if __name__ == "__main__":
    cli = DatabaseCli()
    cli.main()
