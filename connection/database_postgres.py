import sys
from sqlalchemy.pool import NullPool
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from interfaces.idatabase import idatabase

sys.path.append("..")


class DBConnectionPostgres(idatabase):
    def __init__(self, username, password, host, port, database) -> None:
        self.__SQLALCHEMY_DATABASE_URL = None
        self.__username = username
        self.__password = password
        self.__host = host
        self.__port = port
        self.__database = database
        self.SQLALCHEMY_DATABASE_URL = None
        self.__encoding = 'UTF-8'

    def set_connection_string(self):
        self.__SQLALCHEMY_DATABASE_URL = f"postgresql+psycopg2://{self.__username}:{self.__password}@{self.__host}:{self.__port}/{self.__database}?client_encoding=utf8"

    def set_engine(self, echo=False, events=True):
        engine = create_engine(
            self.__SQLALCHEMY_DATABASE_URL,
            client_encoding="utf8",
            echo=echo,
            poolclass=NullPool,
        )
        return engine

    def create_session(self, engine, autocommit=False, autoflush=False):
        return sessionmaker(autocommit=autocommit, autoflush=autoflush, bind=engine)
