import pymysql
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from read_data_database import connect_to_db,read_sql_data
from dotenv import load_dotenv
import os



# Function to calculate null values
def calculate_null_values(connection, table, columns):
    """
    Calculates the number of null values in specified columns of a given table.

    Args:
        connection: Database connection object.
        table (str): Name of the table to analyze.
        columns (list): List of columns to check for null values.
    """

    # SQL query to calculate null value counts for each column
    query = f"""
        SELECT 
        {", ".join([f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS Null_{col}_Count" for col in columns])}
        FROM {table};
    """
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    for col, count in zip(columns, result):
        print(f"Null_{col}_Count: {count}")


# Function to calculate min, max, and avg
def calculate_statistics(connection, table, columns):
    """
    Calculates average, minimum, and maximum values for specified columns in a table.

    Args:
        connection: Database connection object.
        table (str): Name of the table to analyze.
        columns (list): List of columns to compute statistics for.
    """

    query = f"""
        SELECT 
        {", ".join([f"AVG({col}) AS Avg_{col}, MAX({col}) AS Max_{col}, MIN({col}) AS Min_{col}" for col in columns])}
        FROM {table};
    """
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    for col in columns:
        print(f"Avg_{col}, Max_{col}, Min_{col}: {result}")


# Function to calculate group-wise statistics
def calculate_group_statistics(connection, table, group_by_col, columns):
    """
    Calculates average values for specified columns grouped by another column.

    Args:
        connection: Database connection object.
        table (str): Name of the table to analyze.
        group_by_col (str): Column to group data by.
        columns (list): List of columns to compute statistics for.
    """
    # SQL query to calculate group-wise averages
    query = f"""
        SELECT {group_by_col},
        {", ".join([f"AVG({col}) AS Avg_{col}" for col in columns])}
        FROM {table}
        GROUP BY {group_by_col};
    """
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    for row in results:
        print(row)


# Function to plot a heatmap
def plot_heatmap(social_media_dataframe):
    """
    Plots a heatmap showing the correlation between numerical columns in a DataFrame.

    Args:
        social_media_dataframe (pd.DataFrame): DataFrame containing the data.
    """

    corr_matrix = social_media_dataframe.corr()
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm')
    plt.title('Correlation Heatmap')
    plt.show()



if __name__ == "__main__":
    load_dotenv()
    host= os.getenv('HOST')
    database= os.getenv('DATABASE')
    user= os.getenv('USER')
    password= os.getenv('PASSWORD')
    connection= connect_to_db(host,database,user,password)
    tables = ['Matrics', 'Audience', 'Platform', 'Post']
    post_columns = ['Post_Type']
    metrics_columns = ['Likes', 'Comments', 'Shares', 'Reach', 'Engagement_Rate']
    calculate_null_values(connection,tables[0],metrics_columns)
    calculate_statistics(connection,tables[0],metrics_columns)
    calculate_group_statistics(connection, tables[0],post_columns[0],metrics_columns )
    plot_heatmap(read_sql_data(connection))
