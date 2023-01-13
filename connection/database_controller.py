import sys
from interfaces.idatabase import idatabase

from .database_postgres import DBConnectionPostgres

sys.path.append("..")


class DBConnectionController(idatabase):
    def __init__(self, type_db, db_conn) -> None:
        self.__type_db = type_db
        self.__db_conn = db_conn

    def set_connection_string(self, username, password, host, port, database):
        if self.__type_db == "postgres":
            self.__db_conn = DBConnectionPostgres(username, password, host, port, database)

        self.__db_conn.set_connection_string()

    def set_engine(self, echo=False, events=True):
        return self.__db_conn.set_engine(echo=False, events=True)

    def create_session(self, engine):
        SessionLocal = self.__db_conn.create_session(engine)
        return SessionLocal
