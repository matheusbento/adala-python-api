from connection.database import generate_session


def get_db():
    session_local = generate_session()
    db = session_local()
    try:
        yield db
    finally:
        db.close()
