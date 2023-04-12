from threading import Lock

import sqlalchemy
import sqlalchemy.orm
from sqlalchemy import create_engine, MetaData

from db.modelbase import Base
from configuration.config import Config


class PostgresDbSessionMeta(type):
    """
        This is a thread-safe implementation of Singleton.
    """
    _instances = {}
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class DbSession(metaclass=PostgresDbSessionMeta):
    """
    Manages Postgres DB sessions
    """
    value: str = None

    def __init__(self, value: str) -> None:
        self.value = value

    factory = None
    engine = None
    metadata = None

    def global_init(self):
        """
        Initializes the global instance of the DbSession
        """
        config = Config()
        if DbSession.factory:
            return
        conn_string = config.POSTGRES_DATABASE_URI
        self.engine = create_engine(conn_string, echo=True, future=True)
        Base.metadata.create_all(self.engine)
        DbSession.engine = self.engine
        DbSession.factory = sqlalchemy.orm.sessionmaker(bind=self.engine)
        DbSession.metadata = [value for key, value in Base.metadata.tables.items()]


def get_postgres_db_session():
    postgres_session = DbSession(value='api_session')
    postgres_session.global_init()
