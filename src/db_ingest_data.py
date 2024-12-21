import mysql.connector
from data_preprocessing import preprocess_data,read_social_media_data

# Database connection
def get_db_connection(hots,database,user,password):
    connection= mysql.connector.connect(
            host=hots,
            database=database,
            user=user,
            password=password
        )
    return connection

# Create tables
def create_table(host,database,user,password,table_name,columns):
    with get_db_connection(host,database,user,password) as conn:
        cursor = conn.cursor()

        # Generate column definitions string from the columns dictionary
        column_definitions = ", ".join([f"{col} {dtype}" for col, dtype in columns.items()])
        print(column_definitions)

        # Create the SQL query
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {column_definitions}
        );
        """

        # Execute the query
        cursor.execute(query)
        conn.commit()
        conn.close()
        cursor.close()
        print(f"Table '{table_name}' created successfully.")


def prepare_data(dataframe, columns):
    return [tuple(row[col] for col in columns) for _, row in dataframe.iterrows()]

def insert_data(host, database, user, password, table_name, data, columns):
    with get_db_connection(host,database,user,password) as conn:
        cursor = conn.cursor()
        try:
            # Generate the placeholders for the values
            placeholders = ", ".join(["%s"] * len(columns))
            query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES ({placeholders})
            """

            cursor.executemany(query, data)
            conn.commit()
            print(f"Data inserted successfully into table '{table_name}'.")
        except mysql.connector.Error as err:
            print(f"Error inserting data: {err}")
        
        finally:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    data_path= '/Users/sachinmishra/Desktop/Social_media/Data/social_media_engagement_data.csv'
    localhost= 'localhost'
    database= 'social_media'
    user= 'root'
    password= 'password'
    table_name= 'x'
    columns = {
            "MatricsID": "INT PRIMARY KEY",
            "Like": "INT",
            "Comment": "INT",
            "Share": "INT",
            "Reach": "INT",
            "Engagement_Rate": "INT",
            "Audience_Age": "INT",
            "Post_Type": "VARCHAR(255)",
            "Platform_Name": "VARCHAR(255)"
        }
    
    # Insert data into Matrics table
    #matrics_columns = ["Matrics_ID", "Likes", "Comments", "Shares", "Reach", "Engagement_Rate", "Audience_Age", "Post_Type", "Platform"]
    get_db_connection(localhost,database,user,password)
    create_table(localhost,database,user,password,table_name,columns)
    social_media_dataframe= read_social_media_data(data_path)
    linkedin_data = preprocess_data(social_media_dataframe)
    table_data= prepare_data(linkedin_data,columns.keys())
    insert_data(localhost,database,user,password,table_name,table_data,columns.keys())


