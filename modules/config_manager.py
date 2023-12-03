import configparser


class ConfigManager:
    @staticmethod
    def start_config():
        config = configparser.ConfigParser()

        with open("config.ini", "w") as configfile:
            config.write(configfile)

    @staticmethod
    def save_mysql_config(credentials):
        config = configparser.ConfigParser()
        config.read("config.ini")
        config["mysql"] = credentials

        with open("config.ini", "w") as configfile:
            config.write(configfile)
        print("Credentials saved to 'config.ini'.")

    @staticmethod
    def load_mysql_config():
        config = configparser.ConfigParser()
        config.read("config.ini")
        if config.has_section("mysql"):
            credentials = {
                "user": config.get("mysql", "user"),
                "password": config.get("mysql", "password"),
                "host": config.get("mysql", "host"),
                "port": config.get("mysql", "port"),
            }
            return credentials
        else:
            return {}

    @staticmethod
    def save_directory_config(directory: str):
        config = configparser.ConfigParser()
        config.read("config.ini")
        config["directory"] = {"directory": directory}

        with open("config.ini", "w") as configfile:
            config.write(configfile)
        print("Directory saved")

    @staticmethod
    def load_directory_config() -> str:
        config = configparser.ConfigParser()
        config.read("config.ini")
        if config.has_section("directory"):
            return config.get("directory", "directory")
        ConfigManager.save_directory_config("./database")
