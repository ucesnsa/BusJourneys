import sqlalchemy as db
from sqlalchemy import exc


def drop_db_table(db_name, tbl_name):
    try:
        print ('Dropping table ', db_name +'.'+tbl_name)
        engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/'+db_name)
        sql = 'DROP TABLE IF EXISTS ' + str(tbl_name) + ';'
        engine.execute(sql)
        print ('table dropped ', db_name +'.'+tbl_name)
    except exc.SQLAlchemyError:
        exit('drop_db_table', "Encountered general SQLAlchemyError!")


def write_to_db_table(df, db_name, tbl_name):
    try:
        from sqlalchemy import create_engine
        engine = db.create_engine('postgresql+psycopg2://postgres:20081979@localhost/'+db_name)
        df.to_sql(tbl_name, engine, if_exists='append')
        #print ('results table created ', db_name +'.'+tbl_name)
        #print ('count:', len(df))
    except exc.SQLAlchemyError:
        exit('write_to_db_table',"Encountered general SQLAlchemyError!")


