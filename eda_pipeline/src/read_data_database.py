import pymysql
from dotenv import load_dotenv
import os
import pandas as pd

# Function to connect to the database
def connect_to_db(hots,database,user,password):
    try:
        conn = pymysql.connect(
            host=hots,
            database=database,
            user=user,
            password=password
        )
        print("Database connection successful!")
        return conn
    except pymysql.MySQLError as e:
        print(f"Error connecting to database: {e}")
        return None
    
# Function to fetch table schemas
def fetch_table_schema(connection, tables):
    for table in tables:
        print(f"Schema of {table} table:")
        query = f"DESCRIBE {table};"
        cursor = connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        for column in results:
            print(column)
        print("\n")

# Function to read data from the database
def read_sql_data(connection):
    query = "SELECT Likes, Comments, Shares, Reach, Engagement_Rate FROM Matrics;"
    datframe = pd.read_sql(query, connection)
    return datframe

if __name__ == "__main__":
    load_dotenv()
    host= os.getenv('HOST')
    database= os.getenv('DATABASE')
    user= os.getenv('USER')
    password= os.getenv('PASSWORD')
    conn= connect_to_db(host,database,user,password)
    tables = ['Matrics', 'Audience', 'Platform', 'Post']
    fetch_table_schema(conn,tables)
