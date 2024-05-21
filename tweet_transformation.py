#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import os
import numpy as np
import s3fs


def create_fs_for_s3():
    aws_access_key = ''
    aws_secret_key = ''
    bucket_name = 'twitterdata-analytics'
    # Create an S3 filesystem object
    # Use anon=False to use your AWS credentials
    fs = s3fs.S3FileSystem(
        anon=False, key=aws_access_key, secret=aws_secret_key)
    return (fs, bucket_name)


def read_data_from_s3():
    fs, bucket_name = create_fs_for_s3()
    # List contents of the root of your bucket

    # # Read a file from the bucket
    with fs.open(f'{bucket_name}/raw_data/tweets.csv', 'rb') as f:
        twitter_df = pd.read_csv(f)
    return (twitter_df)


def validate_dataframe(df):
    errors = []

    # Check for missing values
    if df.isnull().any().any():
        errors.append("Dataframe contains missing values.")

    # Validate 'author' and 'content' (non-empty strings)
    for column in ['author', 'content']:
        if df[column].apply(lambda x: not isinstance(x, str) or not x.strip()).any():
            errors.append(f"Column '{column}' should be non-empty strings.")

    # Validate 'date_time' (correct datetime format)
    try:
        pd.to_datetime(df['date_time'])
    except ValueError:
        errors.append("Column 'date_time' contains invalid datetime format.")

    # Validate 'id' (unique)
    if df['id'].duplicated().any():
        errors.append("Column 'id' should contain unique values.")

    # Validate 'language' (use a set of valid ISO language codes)
    valid_languages = {'en', 'es', 'fr', 'de'}  # example set of languages
    if not df['language'].isin(valid_languages).all():
        errors.append("Column 'language' contains invalid language codes.")

    # Validate 'latitude' and 'longitude'
    if not df['latitude'].apply(lambda x: -90 <= x <= 90).all():
        errors.append("Column 'latitude' contains out of range values.")
    if not df['longitude'].apply(lambda x: -180 <= x <= 180).all():
        errors.append("Column 'longitude' contains out of range values.")

    # Validate 'number_of_likes' and 'number_of_shares' (non-negative integers)
    for column in ['number_of_likes', 'number_of_shares']:
        if df[column].apply(lambda x: not isinstance(x, int) or x < 0).any():
            errors.append(
                f"Column '{column}' should be non-negative integers.")

    return errors


def fix_dataframe(df):
    # Fill missing values
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    string_cols = df.select_dtypes(include=[object]).columns
    df[string_cols] = df[string_cols].fillna('Unknown')

    df['date_time'] = pd.to_datetime(df['date_time'], format="%d/%m/%Y %H:%M")
    # Ensure 'id' is unique
    while df['id'].duplicated().any():
        df.loc[df['id'].duplicated(), 'id'] = pd.util.hash_pandas_object(
            df[df['id'].duplicated()])

    # Set a default language for invalid language codes
    valid_languages = {'en', 'es', 'fr', 'de'}
    df['language'] = df['language'].apply(
        lambda x: x if x in valid_languages else 'en')

    # Correct 'latitude' and 'longitude' out of range
    df['latitude'] = df['latitude'].clip(-90, 90)
    df['longitude'] = df['longitude'].clip(-180, 180)

    return df


def mode_function(series):
    return series.mode().iloc[0] if not series.mode().empty else None


def mean_rounded(series, decimals=0):
    return round(series.mean(), decimals)


def load_to_s3_bucket(df):
    fs, bucket_name = create_fs_for_s3()
    path = f's3://{bucket_name}/output_data/hourly_output.csv'
    with fs.open(path, 'w', newline='') as f:
        df.to_csv(f, index=False)


def extract_transform_load():
    tweet_df = read_data_from_s3()
    validation_errors = validate_dataframe(tweet_df)

    cleaned_tweets_df = fix_dataframe(tweet_df)
    cleaned_tweets_df['hour'] = cleaned_tweets_df['date_time'].dt.hour
    hourly_tweet_df = cleaned_tweets_df.groupby(['hour']).agg({'number_of_likes': mean_rounded,
                                                               'number_of_shares': mean_rounded,
                                                               'author': mode_function})

    hourly_tweet_df = hourly_tweet_df.reset_index()
    load_to_s3_bucket(hourly_tweet_df)
