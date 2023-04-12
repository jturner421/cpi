from sqlalchemy import MetaData, Table

from db.dbsession import DbSession


def nos_group_lookup(table_name):
    session = DbSession.factory()
    results = session.query(table_name).all()
    lookup = [{row.nos_code: row.nos_group} for row in results]
    return lookup


def get_reflected_tables():
    engine = DbSession.engine
    metadata_obj = MetaData()
    nos = Table('nos', metadata_obj, autoload=True, autoload_with=engine)
    deadlines = Table("deadlines", metadata_obj, autoload_with=engine)
    DbSession.metadata = metadata_obj
    return nos, deadlines
