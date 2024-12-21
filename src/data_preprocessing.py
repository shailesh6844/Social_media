import pandas as pd
import uuid

def read_social_media_data(data_path):
    # Load data
    social_media_data = pd.read_csv(data_path)

    return social_media_data

def preprocess_data(social_media_dataframe):
    linkedin_data = social_media_dataframe[social_media_dataframe["Platform"] == "LinkedIn"]
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



if __name__ == '__main__':
    data_path= '/Users/sachinmishra/Desktop/Social_media/Data/social_media_engagement_data.csv'
    social_media_dataframe= read_social_media_data(data_path)
    linkedin_data = preprocess_data(social_media_dataframe)