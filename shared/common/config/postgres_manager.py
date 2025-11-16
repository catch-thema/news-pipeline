import psycopg2
import contextlib
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from .db_config import BaseDBManager

class PostgresDBManager(BaseDBManager):
    def __init__(self, connection_params):
        super().__init__(connection_params)
        self.engine = create_engine(
            f"postgresql+psycopg2://{connection_params['user']}:{connection_params['password']}"
            f"@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}",
            pool_pre_ping=True,
            pool_recycle=3600,
            echo=False,
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.Base = declarative_base()

    @contextlib.contextmanager
    def get_connection(self):
        conn = psycopg2.connect(**self.connection_params)
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