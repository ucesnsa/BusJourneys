from mylib import db_connection as db
from sqlalchemy import exc


def drop_db_table(db_name, tbl_name):
    try:
        print ('Dropping table ', db_name +'.'+tbl_name)
        db_obj = db.DBConnection()
        sql = 'DROP TABLE IF EXISTS ' + str(tbl_name) + ';'
        db_obj.conn.execute(sql)
        print ('table dropped ', db_name +'.'+tbl_name)
    except exc.SQLAlchemyError:
        exit('drop_db_table', "Encountered general SQLAlchemyError!")


def write_to_db_table(df, db_name, tbl_name):
    try:
        db_obj = db.DBConnection()
        df.to_sql(tbl_name, db_obj.conn, if_exists='append')
        #print ('results table created ', db_name +'.'+tbl_name)
        #print ('count:', len(df))
    except exc.SQLAlchemyError:
        exit('write_to_db_table',"Encountered general SQLAlchemyError!")


