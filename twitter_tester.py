import os
from tweet_objects import Tweet
from twitter_redis import TwitterReddisAPI
from datetime import datetime
import pandas as pd
import csv


follows_csv = "follows.csv"

def get_post_tweets(api, csv_filename):
    """
    Function to organize the retreival of tweets from the csv and calling the api to post
    :param api: the MYSQL connection object
    :param csv_filename: the file name of the csv
    :return: the calulated time using the calculate_time() function
    """
    post_counter = 0

    # open up tweets csv file
    with open(csv_filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        # loop through each tweet (row)
        for row in reader:
            if post_counter == 0:
                start_timestamp = datetime.now()

            # define a tweet object
            tweet_obj = Tweet(row['USER_ID'], row['TWEET_TEXT'])

            # call the api to post the tweet
            api.post_tweet(tweet_obj)
            post_counter+=1

        end_timestamp = datetime.now()

    # call calculate_time with the start timestamp, end timestamp, and the tweet post counter
    return calculate_time(start_timestamp, end_timestamp, post_counter)



def calculate_time(start_timestamp, end_timestamp, counter):
    """

    :param start_timestamp: the starting timestamp
    :param end_timestamp: the ending timestamp
    :param counter: the action count (such as tweets posted)
    :return: the counts per second
    """
    # find the elapsed time
    time_difference = end_timestamp - start_timestamp

    # print these out for the user
    print(f"Count Number: {counter}")
    print(f"time elapsed: {time_difference}")

    # calculate counts per second by doing counts divided by total seconds
    return counter/time_difference.total_seconds()

def get_timelines(api, count):
    """

    :param api: the MYSQL connection object
    :param count: total number of timelines to retrieve
    :return: list of timelines, calculated counts per second
    """
    timeline_list = []
    start_timestamp = datetime.now()

    # loop the number of times we have to retreive a new home timeline
    for i in range(count):
        # call the api to get a random user id
        random_id = api.get_random_ID(follows_csv)

        # call the api to find the timeline for that user
        timeline = api.get_timeline(random_id)

        # append the timeline to the list of timelines
        timeline_list.append(timeline)

    end_timestamp = datetime.now()
    calculated_time = calculate_time(start_timestamp, end_timestamp, count)

    return timeline_list, calculated_time

def main():

    # Make the redis connection
    api = TwitterReddisAPI()

    # call the flush all api method
    api.flush_all()

    # update the followers in redis
    df = pd.read_csv('follows.csv')
    for idx, row in df.iterrows():
        api.update_followers(int(row['USER_ID']), int(row['FOLLOWS_ID']))

    # run the post tweets function, and collect the tweets per second
    tweets_second = get_post_tweets(api, "tweet.csv")
    print("Posts per second", tweets_second)

    # run the timelines function, and get the timelines list and the timelines per second
    timelines, timelines_per_second = get_timelines(api, 1000)

    # set the pandas option to not hide any columns when printing dataframe
    pd.set_option('display.max_columns', None)
    print("Timelines per Second: ", timelines_per_second)

    # print the first timeline only (theres too many timelines to print all)
    print(timelines[0])

    # Close the redis connection
    api.close_redis_connection()



if __name__ == '__main__':
    main()