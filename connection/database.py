import os
import string

from dotenv import dotenv_values
from connection import DBConnectionController

config = dict(dotenv_values(".env"))

USERNAME = config['USERNAME']
PASSWORD = config['PASSWORD']
HOST = config['HOST']
PORT = config['PORT']
DATABASE = config['DB_NAME']
TYPE_DB = config['TYPE_DB']


def generate_session(database_name: string = None):
    if database_name is None:
        database_name = DATABASE
    dbConn = DBConnectionController(TYPE_DB, None)
    dbConn.set_connection_string(USERNAME, PASSWORD, HOST, PORT, database_name)
    engine = dbConn.set_engine(echo=False, events=True)
    return dbConn.create_session(engine)
