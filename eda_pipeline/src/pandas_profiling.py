import pandas as pd
from ydata_profiling import ProfileReport
import os


def create_pandas_profile_report(dataframe):
        profile = ProfileReport(dataframe, title="Social Media Pandas Profiling Report", explorative=True)
        # Construct the path to the 'Data' folder
        root_dir = os.path.dirname(os.path.abspath(__file__))  # Get the current script directory
        data_folder = os.path.join(root_dir, "../../Data")     # Navigate to the 'Data' folder relative to the script
        # Save the report in the Data folder
        output_path = os.path.join(data_folder, "social_media_eda_report.html")

        profile.to_file(output_path)

if __name__ == "__main__":
        dataframe= pd.read_csv('/Users/sachinmishra/Desktop/Social_media/Data/social_media_engagement_data.csv')
        create_pandas_profile_report(dataframe)