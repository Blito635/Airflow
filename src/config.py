import os

class Settings:
    """ Configuracion de las variables de entorno"""

    SOURCE_DB_URL = os.getenv("SOURCE_DB_URL")
    TARGET_DB_URL = os.getenv("TARGET_DB_URL")

settings = Settings()
