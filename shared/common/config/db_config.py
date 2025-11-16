# shared/config/db_config.py
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

class BaseDBManager:
    def __init__(self, connection_params):
        self.connection_params = connection_params

    @contextmanager
    def get_connection(self):
        raise NotImplementedError("Subclass must implement `get_connection` method")

    def close(self):
        raise NotImplementedError("Subclass must implement `close` method")