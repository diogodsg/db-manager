import os
from colorama import Fore, Style
from modules.config_manager import ConfigManager


class DirectoryManager:
    def __init__(self) -> None:
        self.directory = ConfigManager.load_directory_config()
        self.ensure_directory_exists()
        if not self.directory:
            self.directory = "./database"

    def ensure_directory_exists(self):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
            print(
                f"{Fore.GREEN}Diretório '{self.directory}' criado com sucesso.{Style.RESET_ALL}"
            )

    def alter_directory(self):
        dir = input(f"{Fore.YELLOW}Digite o novo diretório: {Style.RESET_ALL}").strip()
        self.directory = dir
        self.ensure_directory_exists()
        ConfigManager.save_directory_config(dir)
