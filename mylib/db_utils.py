import sqlalchemy as db


def drop_db_table(db_name, tbl_name):
    print ('Dropping table ', db_name +'.'+tbl_name)
    from sqlalchemy import create_engine
    engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/'+db_name)
    sql = 'DROP TABLE IF EXISTS ' + str(tbl_name) + ';'
    engine.execute(sql)
    print ('table dropped ', db_name +'.'+tbl_name)

def write_to_db_table(df, db_name, tbl_name):
    from sqlalchemy import create_engine
    engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/'+db_name)
    df.to_sql(tbl_name, engine, if_exists='append')
    print ('results table created ', db_name +'.'+tbl_name)
    print ('count:', len(df))

