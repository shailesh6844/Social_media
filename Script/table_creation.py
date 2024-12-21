from establish_connections import connection

def table_creation():

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

table_creation()

print("Table Created")

