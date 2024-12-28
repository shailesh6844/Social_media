import mysql.connector
from src.data_preprocessing import preprocess_data,read_social_media_data
from src.db_ingest_data import get_db_connection,create_table,insert_data,prepare_data
from dotenv import load_dotenv
import os



if __name__ == '__main__':
    load_dotenv() 

    data_path=r'C:/Users/shailesh shinde/Desktop/Social_media/Data/social_media_engagement_data.csv'
    host= os.getenv('HOST')
    database= os.getenv('DATABASE')
    user= os.getenv('USER')
    password= os.getenv('PASSWORD')
    table_name= os.getenv('TABLE_NAME')
    matrics_columns = {
            "Matrics_ID": "INT PRIMARY KEY",
            "Likes": "INT",
            "Comments": "INT",
            "Shares": "INT",
            "Reach": "INT",
            "Engagement_Rate": "INT",
            "Audience_Age": "INT",
            "Post_Type": "VARCHAR(255)",
            "Platform": "VARCHAR(255)"
        }
    print("---")
    print(matrics_columns.keys())
    
    audience_columns= {"AudienceID": "INT PRIMARY KEY",
        "Audience_Age": "INT",
        "Audience_Gender": "VARCHAR(255)",
        "Audience_Location": "VARCHAR(255)",
        "Audience_Interests": "VARCHAR(255)"
        }
    
    platform_columns= {"PlatformID": "INT PRIMARY KEY",
    "Campaign_ID": "VARCHAR(255)",
    "Platform": "VARCHAR(255)"}

    post_columns= {
        "Post_id": "VARCHAR(255) PRIMARY KEY",
        "Post_Type": "VARCHAR(255)",
        "Post_Content": "TEXT",
        "Post_Timestamp": "TIMESTAMP"
    }



    # Insert data into Matrics table
    #matrics_columns = ["Matrics_ID", "Likes", "Comments", "Shares", "Reach", "Engagement_Rate", "Audience_Age", "Post_Type", "Platform"]
    get_db_connection(host,database,user,password)
    create_table(host,database,user,password,table_name,post_columns)
    social_media_dataframe= read_social_media_data(data_path)
    linkedin_data = preprocess_data(social_media_dataframe)
    table_data= prepare_data(linkedin_data,post_columns.keys())
    insert_data(host,database,user,password,table_name,table_data,post_columns.keys())