import pandas as pd
import numpy as np
import ast

folder_path = '../data/processed/'

def read_channel_df():
    path = folder_path + 'df_channels_processed.csv'
    df_channels = pd.read_csv(path)
    df_channels['join_date'] = pd.to_datetime(df_channels['join_date'])
    return df_channels

def read_video_df():
    path = folder_path + 'df_videos_processed.csv'
    df_videos = pd.read_csv(path)
    df_videos['published'] = pd.to_datetime(df_videos['published'])
    df_videos['duration'] = pd.to_timedelta(df_videos['duration'])
    df_videos['tags'] = df_videos['tags'].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) else np.nan)

    return df_videos

def read_comment_df():
    path = folder_path + 'df_comments_processed.csv'
    df_comments = pd.read_csv(path, lineterminator='\n')
    df_comments['published_at'] =  df_comments['published_at'].apply(pd.to_datetime)
    df_comments['updatedat'] =  df_comments['updatedat'].apply(pd.to_datetime)
    return df_comments

