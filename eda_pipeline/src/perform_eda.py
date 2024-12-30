import pymysql
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


# Function to connect to the database
def connect_to_db():
    try:
        conn = pymysql.connect(
            host='localhost',
            database='social_media',
            user='root',
            password='6844'
        )
        print("Database connection successful!")
        return conn
    except pymysql.MySQLError as e:
        print(f"Error connecting to database: {e}")
        return None


# Function to fetch table schemas
def fetch_table_schema(cursor, tables):
    for table in tables:
        print(f"Schema of {table} table:")
        query = f"DESCRIBE {table};"
        cursor.execute(query)
        results = cursor.fetchall()
        for column in results:
            print(column)
        print("\n")


# Function to calculate null values
def calculate_null_values(cursor, table, columns):
    query = f"""
        SELECT 
        {", ".join([f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS Null_{col}_Count" for col in columns])}
        FROM {table};
    """
    cursor.execute(query)
    result = cursor.fetchone()
    for col, count in zip(columns, result):
        print(f"Null_{col}_Count: {count}")


# Function to calculate min, max, and avg
def calculate_statistics(cursor, table, columns):
    query = f"""
        SELECT 
        {", ".join([f"AVG({col}) AS Avg_{col}, MAX({col}) AS Max_{col}, MIN({col}) AS Min_{col}" for col in columns])}
        FROM {table};
    """
    cursor.execute(query)
    result = cursor.fetchone()
    for col in columns:
        print(f"Avg_{col}, Max_{col}, Min_{col}: {result}")


# Function to calculate group-wise statistics
def calculate_group_statistics(cursor, table, group_by_col, columns):
    query = f"""
        SELECT {group_by_col},
        {", ".join([f"AVG({col}) AS Avg_{col}" for col in columns])}
        FROM {table}
        GROUP BY {group_by_col};
    """
    cursor.execute(query)
    results = cursor.fetchall()
    for row in results:
        print(row)


# Function to plot a heatmap
def plot_heatmap(df):
    corr_matrix = df.corr()
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm')
    plt.title('Correlation Heatmap')
    plt.show()


# Pipeline function to run all tasks
def pipeline():
    conn = connect_to_db()
    if conn is None:
        return

    cursor = conn.cursor()

    # Example tables and columns
    tables = ['Matrics', 'Audience', 'Platform', 'Post']
    metrics_columns = ['Likes', 'Comments', 'Shares', 'Reach', 'Engagement_Rate']

    # Fetch schemas
    fetch_table_schema(cursor, tables)

    # Null values in Matrics
    calculate_null_values(cursor, 'Matrics', metrics_columns)

    # Min, Max, Avg in Matrics
    calculate_statistics(cursor, 'Matrics', metrics_columns)

    # Group-wise stats
    calculate_group_statistics(cursor, 'Matrics', 'Post_Type', metrics_columns)

    # Plot heatmap
    query = "SELECT Likes, Comments, Shares, Reach, Engagement_Rate FROM Matrics;"
    df = pd.read_sql(query, conn)
    plot_heatmap(df)

    cursor.close()
    conn.close()


if __name__ == "__main__":
    pipeline()
