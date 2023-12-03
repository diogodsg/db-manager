import os
from colorama import Fore, Style


class DirectoryManager:
    def __init__(self, default: str = "./database") -> None:
        self.directory = default

    def ensure_directory_exists(self):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
            print(
                f"{Fore.GREEN}Directory '{self.directory}' created successfully.{Style.RESET_ALL}"
            )

    def alter_directory(self, dir: str):
        self.directory = dir
        self.ensure_directory_exists(dir)
