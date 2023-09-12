from sqlalchemy.orm import Session
from psycopg2 import sql
import sys
from sqlalchemy import create_engine, text

from connection.database import generate_session

sys.path.append("..")


class SchemaRepository:

    def __init__(self, db: Session, schema_name):
        self.db = db
        self.schema_name = schema_name

    def create_schema(self):
        session = self.db.connection()
        # sql_exists = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}'".format(
        #     schema_name=self.schema_name)
        # # sql_exists = "DROP DATABASE {schema_name};".format(
        # #     schema_name=self.schema_name.lower())
        # print(123123, sql_exists)
        # self.db.connection().connection.set_isolation_level(0)
        # res = self.db.execute(sql_exists)
        # self.db.connection().connection.set_isolation_level(1)
        # print(res, sql_exists)
        # if res is None:
        #     sql = "CREATE DATABASE {schema} OWNER baslake".format(schema=self.schema_name)
        #     self.db.connection().connection.set_isolation_level(0)
        #     self.db.execute(sql)
        #     self.db.connection().connection.set_isolation_level(1)
        #
        # session = generate_session(self.schema_name)
        # return session()
        exists = session.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :schema_name"),
            {'schema_name': self.schema_name}).fetchone()

        # Se o schema não existir, crie-o
        if not exists:
            session.execute(text("COMMIT"))
            session.execute(text(f"CREATE DATABASE {self.schema_name}"))
            print(f"Schema '{self.schema_name}' created.")
        else:
            print(f"Schema '{self.schema_name}' already exists.")

        # Fechando o cursor e a conexão
        session.close()
        session = generate_session(self.schema_name)
        return session()
