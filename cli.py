import os
from colorama import Fore, Style
from modules.db_manager import DatabaseManager
from modules.db_connection import DatabaseConnection
from modules.directory_manager import DirectoryManager


class DatabaseCli:
    def __init__(self) -> None:
        self.dm = DirectoryManager()
        self.db_manager = DatabaseManager()
        self.db_connection = DatabaseConnection(self.dm.directory)

    def display_options(self):
        print("\nOpções:")
        print(f"{Fore.CYAN}0. Listar Tabelas{Style.RESET_ALL}")
        print(f"{Fore.CYAN}1. Executar Query{Style.RESET_ALL}")
        print(f"{Fore.CYAN}2. Importar do MySQL{Style.RESET_ALL}")
        print(f"{Fore.CYAN}3. Importar de CSV{Style.RESET_ALL}")
        print(f"{Fore.CYAN}4. Conectar com MySQL{Style.RESET_ALL}")
        print(f"{Fore.CYAN}5. Alterar Diretório{Style.RESET_ALL}")

    def main(self):
        print(
            f"{Fore.YELLOW}Bem vindo a CLI do gerenciador de banco de dados!{Style.RESET_ALL}"
        )
        print(
            f"O diretório do seu banco de dados está setado para ser: {Fore.MAGENTA}{self.dm.directory}{Style.RESET_ALL}!"
        )

        while True:
            try:
                self.display_options()
                choice = input(f"{Fore.YELLOW}Digite a opção (0-5): {Style.RESET_ALL}")
                if choice == "0":
                    self.db_manager.list_tables(self.dm.directory)

                elif choice == "1":
                    self.execute_queries()
                elif choice == "2":
                    self.db_connection.import_from_database()
                elif choice == "3":
                    self.db_manager.import_from_csv(self.dm.directory)
                elif choice == "4":
                    self.db_connection.connect()
                elif choice == "5":
                    self.dm.alter_directory()
                else:
                    print(
                        f"{Fore.RED}. Por favor escolha um número entre 1 e 5.{Style.RESET_ALL}"
                    )
            except Exception as e:
                print(f"Error: {e}")

    def execute_queries(self):
        while True:
            print(f"{Fore.RED}Escreva 'exit' para voltar ao menu!{Style.RESET_ALL}")
            query = self.get_user_input(f"Escreva sua query:\n")

            if query.lower().startswith("exit"):
                break

            self.build_and_execute_query(query)

    def get_user_input(self, prompt):
        return input(f"{Fore.YELLOW}{prompt}> {Style.RESET_ALL}").strip()

    def build_and_execute_query(self, initial_query):
        query = initial_query

        while not query.endswith(";"):
            query_part = self.get_user_input("")
            query += " " + query_part

        query = query.replace(";", "")
        self.db_manager.execute_query(self.dm.directory, query)


if __name__ == "__main__":
    cli = DatabaseCli()
    cli.main()
