import pymysql
import contextlib
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from .db_config import BaseDBManager

class MySQLDBManager(BaseDBManager):
    def __init__(self, connection_params):
        super().__init__(connection_params)
        self.engine = create_engine(
            f"mysql+pymysql://{connection_params['user']}:{connection_params['password']}"
            f"@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}?charset=utf8mb4",
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False,
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.Base = declarative_base()

    @contextlib.contextmanager
    def get_connection(self):
        conn = pymysql.connect(
            host=self.connection_params['host'],
            user=self.connection_params['user'],
            password=self.connection_params['password'],
            database=self.connection_params['database'],
            port=self.connection_params.get('port', 3306),
            charset='utf8mb4'
        )
        try:
            yield conn
        finally:
            conn.close()

    def get_session(self):
        db = self.SessionLocal()
        try:
            yield db
        finally:
            db.close()

    def close(self):
        self.engine.dispose()

