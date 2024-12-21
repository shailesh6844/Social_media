import pandas as pd
import uuid
import mysql.connector

# Load data
df = pd.read_csv(r"C:/Users/shailesh shinde/OneDrive/Desktop/Final Project/Data/social_media_engagement_data.csv")

# Filter and preprocess LinkedIn data
def preprocess_data(df):
    linkedin_data = df[df["Platform"] == "LinkedIn"]
    linkedin_data['AudienceID'] = range(1, len(linkedin_data) + 1)
    linkedin_data['MatricsID'] = range(1, len(linkedin_data) + 1)
    linkedin_data['PlatformID'] = range(1, len(linkedin_data) + 1)
    linkedin_data = linkedin_data.rename(columns={
        "Post ID": "Post_id",
        "Post Type": "Post_Type",
        "Post Content": "Post_Content",
        "Post Timestamp": "Post_Timestamp",
        "Engagement Rate": "Engagement_Rate",
        "Audience Age": "Audience_Age",
        "Audience Gender": "Audience_Gender",
        "Audience Location": "Audience_Location",
        "Audience Interests": "Audience_Interests",
        "Campaign ID": "Campaign_ID",
        "Influencer ID": "Influencer_ID",
        "MatricsID": "Matrics_ID"
    })
    linkedin_data["Campaign_ID"] = linkedin_data["Campaign_ID"].apply(
        lambda x: str(uuid.uuid4()) if pd.isna(x) or x == "" else x
    )
    return linkedin_data

Linkedindata = preprocess_data(df)

# Database connection
def get_db_connection():
    return mysql.connector.connect(
        host='localhost',
        database='try_functions',
        user='root',
        password='6844'
    )

# Create tables
def create_table(query):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()

# Insert data
def insert_data(query, data):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.executemany(query, data)
            conn.commit()
        except mysql.connector.Error as err:
            print(f"Error inserting data: {err}")

# Prepare data for insertion
def prepare_data(dataframe, columns):
    return [tuple(row[col] for col in columns) for _, row in dataframe.iterrows()]

# Create tables
create_table('''
CREATE TABLE IF NOT EXISTS Matrics (
    MatricsID INT PRIMARY KEY,
    `Like` INT,
    Comment INT,
    Share INT,
    Reach INT,
    Engagement_Rate INT,
    Audience_Age INT,
    Post_Type VARCHAR(255),
    Platform_Name VARCHAR(255)
);
''')
print("Matrics table created successfully")

create_table('''
CREATE TABLE IF NOT EXISTS Audience (
    AudienceID INT PRIMARY KEY,
    Audience_Age INT,
    Audience_Gender VARCHAR(255),
    Audience_Location VARCHAR(255),
    Audience_Interests VARCHAR(255)
);
''')
print("Audience table created successfully")

create_table('''
CREATE TABLE IF NOT EXISTS Platform (
    PlatformID INT PRIMARY KEY,
    Campaign_ID VARCHAR(255),
    PlatformName VARCHAR(255)
);
''')
print("platform table created successfully")

create_table('''
CREATE TABLE IF NOT EXISTS Post (
    PostID VARCHAR(255) PRIMARY KEY,
    Post_Type VARCHAR(255),
    Post_Content TEXT,
    Post_Timestamp TIMESTAMP
);
''')
print("Post table created successfully")

# Insert data into Matrics table
matrics_columns = ["Matrics_ID", "Likes", "Comments", "Shares", "Reach", "Engagement_Rate", "Audience_Age", "Post_Type", "Platform"]
matrics_data = prepare_data(Linkedindata, matrics_columns)
insert_data("""
    INSERT INTO Matrics (MatricsID, `Like`, Comment, Share, Reach, Engagement_Rate, Audience_Age, Post_Type, Platform_Name)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""", matrics_data)
print("Data inserted successfully into matrics table")

# Insert data into Audience table
audience_columns = ["AudienceID", "Audience_Age", "Audience_Gender", "Audience_Location", "Audience_Interests"]
audience_data = prepare_data(Linkedindata, audience_columns)
insert_data("""
    INSERT INTO Audience (AudienceID, Audience_Age, Audience_Gender, Audience_Location, Audience_Interests)
    VALUES (%s, %s, %s, %s, %s)
""", audience_data)
print("Data inserted successfully into Audience table")

# Insert data into Platform table
platform_columns = ["PlatformID", "Campaign_ID", "Platform"]
platform_data = prepare_data(Linkedindata, platform_columns)
insert_data("""
    INSERT INTO Platform (PlatformID, Campaign_ID, PlatformName)
    VALUES (%s, %s, %s)
""", platform_data)
print("Data inserted successfully into platform table")

# Insert data into Post table
post_columns = ["Post_id", "Post_Type", "Post_Content", "Post_Timestamp"]
post_data = prepare_data(Linkedindata, post_columns)
insert_data("""
    INSERT INTO Post (PostID, Post_Type, Post_Content, Post_Timestamp)
    VALUES (%s, %s, %s, %s)
""", post_data)

print("Data processing and insertion completed successfully!")
