import psycopg2
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)


class DBManager:
    def __init__(self, host, port, database, user, password):
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        self.connection = None

    @contextmanager
    def get_connection(self):
        """DB 커넥션 컨텍스트 매니저"""
        conn = psycopg2.connect(**self.connection_params)
        try:
            yield conn
        finally:
            conn.close()

    def close(self):
        """커넥션 종료"""
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.info("DB 연결 종료")