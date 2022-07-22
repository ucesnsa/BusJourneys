import sqlalchemy as db


def drop_db_table(tbl_name):
    from sqlalchemy import create_engine
    engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/LTDS_Matched_Journeys')
    sql = 'DROP TABLE IF EXISTS ' + str(tbl_name) + ';'
    engine.execute(sql)


def write_to_db_table(df, tbl_name):
    from sqlalchemy import create_engine
    engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/LTDS_Matched_Journeys')
    df.to_sql(tbl_name, engine, if_exists='append')
