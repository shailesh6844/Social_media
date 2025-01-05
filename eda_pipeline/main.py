from dotenv import load_dotenv
import pandas as pd
from ydata_profiling import ProfileReport
import os
from src.read_data_database import connect_to_db,fetch_table_schema
from src.perform_eda import (calculate_null_values,calculate_statistics,
                            calculate_group_statistics,plot_heatmap,read_sql_data)
from src.pandas_profiling import create_pandas_profile_report

if __name__ == '__main__':
    
    #dataframe= pd.read_csv('')
    dataframe = pd.read_csv(r'C:\Users\shailesh shinde\OneDrive\Desktop\Social_media\Data\social_media_engagement_data.csv')

    create_pandas_profile_report(dataframe)
    
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



    