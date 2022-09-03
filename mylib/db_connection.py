import sqlalchemy as db
from sqlalchemy import exc


# singleton

class DBConnection(object):
    conn = None
    def __new__(cls,e):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DBConnection, cls).__new__(cls)
            db_name = 'scd_bus_journeys'
            engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/' + e)
            cls.conn = engine.connect()
        return cls.instance
