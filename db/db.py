import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv()

class Database:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host=os.environ.get("DB_HOST"),
            database=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USER"),
            password=os.environ.get("DB_PASSWORD")
        )


    def create_table(self, table_name, fields):
        cur = self.conn.cursor()
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(fields)});"
        cur.execute(sql)
        self.conn.commit()
        cur.close()

    def create_multiple_tables(self, table_name_list, fields_list):
        cur = self.conn.cursor()
        for table_name, fields in zip(table_name_list, fields_list):
            cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(fields)});")
        self.conn.commit()
        cur.close()
    
    def drop_database(self, database_name):
        cur = self.conn.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS {database_name};")
        self.conn.commit()
        cur.close()
    
    def drop_table(self, table_name):
        cur = self.conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table_name};")
        self.conn.commit()
        cur.close()

    def create_database(self, database_name):
        cur = self.conn.cursor()
        cur.execute(f"CREATE DATABASE {database_name};")
        self.conn.commit()
        cur.close()

    def insert_data(self, table_name, fields, data):
        try:
            cur = self.conn.cursor()
            placeholders = ', '.join(['%s' for _ in fields])
            sql = f""" INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders}) """
            print('sql', sql)
            print('data', data)
            cur.execute(sql, data)
            self.conn.commit()
            cur.close()
        except (Exception, mysql.connector.Error) as error:
            print("Failed inserting record into table: {table_name}".format(error))
        finally:
            # closing database connection.
            if self.conn:
                cur.close()

    def insert_multiple_data(self, table_name, fields, data_list):
        try:
            cur = self.conn.cursor()
            placeholders = ', '.join(['%s' for _ in fields])
            # sql = f""" INSERT INTO {table_name} ({', '.join(fields)}) 
            #     VALUES ({placeholders}) """
            sql = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders})"
            # print('sql',sql)
            # print('data_list',data_list)
            cur.executemany(sql, data_list)
            self.conn.commit()
            # print(cur.rowcount, "Record inserted successfully into mobile table")
        except (Exception, mysql.connector.Error) as error:
            print(f"Failed inserting record into table {table_name} VALUES{data_list}".format(error))
        finally:
            # closing database connection.
            if self.conn:
                cur.close()

    def update_data(self, table_name, data):
        cur = self.conn.cursor()
        cur.execute(f"UPDATE {table_name} SET data = %s WHERE num = %s", data)
        self.conn.commit()
        cur.close()

    def delete_data(self, table_name, num):
        cur = self.conn.cursor()
        cur.execute(f"DELETE FROM {table_name} WHERE num = %s", (num,))
        self.conn.commit()
        cur.close()

    def select_data(self, table_name):
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM {table_name}")
        results = cur.fetchall()
        cur.close()
        return results

    def close(self):
        self.conn.close()
