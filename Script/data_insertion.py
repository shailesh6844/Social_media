from read_data import read_data
from establish_connections import connection

insert_query = """
    INSERT INTO Matrics (MatricsID, Like, Comment, Share, Reach, Engagement_Rate, Audience_Age, Post_Type, Platform_Name)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Iterate through each row in the DataFrame
for _, row in Linkedindata.iterrows():
    try:
        cursor.execute(insert_query, (
            row['MatricsID'],
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