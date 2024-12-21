
from read_data import read_data
import mysql.connector

# Establish connection
def connection():
    conn = mysql.connector.connect(
        host='localhost',
        database='Social_media_project',
        user='root',
        password='6844'
    )

    cursor = conn.cursor()
    print("connection established")


    cursor.execute('''
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
    # Create Audience Table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Audience (
        AudienceID INT PRIMARY KEY,
        Audience_Age INT,
        Audience_Gender VARCHAR(255),
        Audience_Location VARCHAR(255),
        Audience_Interests VARCHAR(255)
    );
    ''')

    # Create Platform Table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Platform (
        PlatformID INT PRIMARY KEY,
        Campaign_Name VARCHAR(255),
        PlatformName VARCHAR(255)
    );
    ''')

    # Create Post Table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS Post (
        PostID INT PRIMARY KEY,
        Post_Type VARCHAR(255),
        Post_Content TEXT,
        Post_Timestamp TIMESTAMP
    );
    ''')


    print("Table Created")

    Linkedindata=read_data()
    cursor = conn.cursor()
 


    insert_query = """
        INSERT INTO Matrics (MatricsID, Like, Comment, Share, Reach, Engagement_Rate, Audience_Age, Post_Type, Platform_Name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Iterate through each row in the DataFrame
    for _, row in Linkedindata.iterrows():
        try:
            cursor.execute(insert_query, (
                row['Matrics_ID'],
                row['Likes'],
                row['Comments'],
                row['Shares'],
                row['Reach'],
                row['Engagement_Rate'],
                row['Audience_Age'],
                row['Post_Type'],
                row['Platform']
            ))
        except Exception as e:
            print(f"Error inserting row: {e}")

    # Commit the transaction
    conn.commit()

    print("Data inserted successfully!")
connection()
