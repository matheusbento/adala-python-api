from sqlalchemy.orm import Session

import sys

from connection.database import generate_session

sys.path.append("..")


class SchemaRepository:

    def __init__(self, db: Session, schema_name):
        self.db = db
        self.schema_name = schema_name

    def create_schema(self):
        sql_exists = "SELECT datname FROM pg_database WHERE datistemplate = false and datname = '{schema_name}'".format(
            schema_name=self.schema_name)
        res = self.db.execute(sql_exists).fetchone()

        if res is None:
            sql = "CREATE DATABASE {schema} OWNER baslake".format(schema=self.schema_name)
            self.db.connection().connection.set_isolation_level(0)
            self.db.execute(sql)
            self.db.connection().connection.set_isolation_level(1)

        session = generate_session(self.schema_name)
        return session()
