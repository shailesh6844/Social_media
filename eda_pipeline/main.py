from dotenv import load_dotenv
import os
from src.read_data_database import connect_to_db,fetch_table_schema
from src.perform_eda import (calculate_null_values,calculate_statistics,
                            calculate_group_statistics,plot_heatmap,read_sql_data)

if __name__ == '__main__':

    load_dotenv()
    host= os.getenv('HOST')
    database= os.getenv('DATABASE')
    user= os.getenv('USER')
    password= os.getenv('PASSWORD')
    connection= connect_to_db(host,database,user,password)
    tables = ['Matrics', 'Audience', 'Platform', 'Post']
    fetch_table_schema(connection,tables)

    tables = ['Matrics', 'Audience', 'Platform', 'Post']
    post_columns = ['Post_Type']
    metrics_columns = ['Likes', 'Comments', 'Shares', 'Reach', 'Engagement_Rate']
    calculate_null_values(connection,tables[0],metrics_columns)
    calculate_statistics(connection,tables[0],metrics_columns)
    calculate_group_statistics(connection, tables[0],post_columns[0],metrics_columns )
    plot_heatmap(read_sql_data(connection))



    