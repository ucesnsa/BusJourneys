import sqlalchemy as db


def drop_db_table():
    from sqlalchemy import create_engine
    engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/LTDS_Matched_Journeys')
    sql = 'DROP TABLE IF EXISTS bus_inference;'
    engine.execute(sql)


def write_to_db_table(df):
    from sqlalchemy import create_engine
    engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/LTDS_Matched_Journeys')
    df.to_sql('bus_inference', engine, if_exists='append')
