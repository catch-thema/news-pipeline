import contextlib
from .postgres_manager import PostgresDBManager
from .mysql_manager import MySQLDBManager
from .settings import settings

postgres_manager = PostgresDBManager({
    'host': settings.postgres_host,
    'port': settings.postgres_port,
    'database': settings.postgres_db,
    'user': settings.postgres_user,
    'password': settings.postgres_password,
})

mysql_manager = MySQLDBManager({
    'host': settings.mysql_host,
    'port': settings.mysql_port,
    'database': settings.mysql_db,
    'user': settings.mysql_user,
    'password': settings.mysql_password,
})

def get_postgres_session():
    db = postgres_manager.SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_mysql_session():
    db = mysql_manager.SessionLocal()
    try:
        yield db
    finally:
        db.close()

@contextlib.contextmanager
def get_postgres_connection():
    with postgres_manager.get_connection() as conn:
        yield conn

@contextlib.contextmanager
def get_mysql_connection():
    with mysql_manager.get_connection() as conn:
        yield conn

postgres_base = postgres_manager.Base
mysql_base = mysql_manager.Base