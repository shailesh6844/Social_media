import pandas as pd
import numpy as np
import matplotlib as plt
import seaborn as sns

def read_data():
    df = pd.read_csv(r"C:/Users/shailesh shinde/OneDrive/Desktop/Final Project/Data/social_media_engagement_data.csv")
    print(df)
    Linkedindata=df[df["Platform"]=="LinkedIn"]
    print(Linkedindata.head());
    Linkedindata['MatricsID'] = range(1, len(Linkedindata) + 1)
    Linkedindata['AudienceID'] = range(1, len(Linkedindata) + 1)
    Linkedindata['PlatformID'] = range(1, len(Linkedindata) + 1)
    Linkedindata=Linkedindata.rename(columns={"Post ID":"Post_id","Post Type":"Post_Type","Post Content":"Post_Content", "Post Timestamp":"Post_Timestamp", 
                                "Engagement Rate":"Engagement_Rate", "Audience Age": "Audience_Age", "Audience Gender": "Audience_Gender",
                                "Audience Location": "Audience_Location", "Audience Interests": "Audience_Interests", "Campaign ID":"Campaign_ID",
                                "Influencer ID": "Influencer_ID", "MatricsID": "Matrics_ID" })
    print(Linkedindata.head())
    return Linkedindata
read_data()
