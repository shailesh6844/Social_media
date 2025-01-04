from dotenv import load_dotenv
import pandas as pd
from ydata_profiling import ProfileReport
import os
from src.read_data_database import connect_to_db,fetch_table_schema
from src.perform_eda import (calculate_null_values,calculate_statistics,
                            calculate_group_statistics,plot_heatmap,read_sql_data)
from src.pandas_profiling import create_pandas_profile_report

if __name__ == '__main__': 
    dataframe= pd.read_csv('/Users/sachinmishra/Desktop/Social_media/Data/social_media_engagement_data.csv')
    create_pandas_profile_report(dataframe)
    # Load environment variables from a .env file
    load_dotenv()

    # Retrieve database credentials from environment variables
    host= os.getenv('HOST')
    database= os.getenv('DATABASE')
    user= os.getenv('USER')
    password= os.getenv('PASSWORD')

    # Establish a connection to the database
    connection= connect_to_db(host,database,user,password)
    
    # List of tables to inspect
    tables = ['Matrics', 'Audience', 'Platform', 'Post']    
    # Fetch and print the schema of each table
    fetch_table_schema(connection,tables)

    # Columns and metrics to analyze in the 'Matrics' table
    post_columns = ['Post_Type']
    metrics_columns = ['Likes', 'Comments', 'Shares', 'Reach', 'Engagement_Rate']
    
    # Perform EDA: Calculate and print the count of null values in specified columns
    calculate_null_values(connection,tables[0],metrics_columns)
    
    # Perform EDA: Calculate and print statistics (min, max, avg) for specified columns
    calculate_statistics(connection,tables[0],metrics_columns)
    
    # Perform EDA: Calculate and print group-wise statistics based on 'Post_Type'
    calculate_group_statistics(connection, tables[0],post_columns[0],metrics_columns )
    
    # Perform EDA: Plot a heatmap to visualize correlations among numeric columns
    plot_heatmap(read_sql_data(connection))



    