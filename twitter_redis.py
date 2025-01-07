import json
from datetime import datetime
import redis
import pandas as pd
import random


class TwitterReddisAPI:

    def __init__(self, host = "localhost", port = 6379, decode_responses = True):
        """

        :param host: the hostname
        :param port: the port number
        :param decode_responses: boolean for decode_responses
        """
        self.redis_connection = redis.Redis(host, port, decode_responses = decode_responses)

    def post_tweet(self, tweet_obj):
        """
        Post the tweet, and update the following users' timelines
        :param tweet_obj: the object of the tweet
        :return: nothing
        """
        # use a hash to post the tweet
        tweet_id = self.redis_connection.incr('tweet_id')
        self.redis_connection.hset(f'tweet:{tweet_id}', mapping={'user_id': tweet_obj.user_id, 'timestamp': datetime.now().isoformat(), 'text': tweet_obj.text})

        # collect the users who follow the user who tweeted
        following = self.redis_connection.smembers(f'following:{tweet_obj.user_id}')

        # update the timeline to add the new posted tweet for each follower
        for user in following:
            self.redis_connection.rpush(f'timeline:{user}', tweet_id)


    def update_followers(self, user_id, follows_id):
        """
        Function to update the set that tracks followers for any given user
        :param user_id: the main user id
        :param follows_id: the id of the user that is following the main user
        :return: nothing
        """
        self.redis_connection.sadd(f'following:{user_id}', follows_id)


    def get_timeline(self, user_id):
        """
        Function given any user id, will return the timeline with all the tweet info
        :param user_id: the user_id of the timeline to retrieve
        :return: the dataframe timeline
        """
        # get the list of tweets in the persons timeline, limited to top 10 most recent
        timeline = self.redis_connection.lrange(f'timeline:{user_id}', 0, 10)

        # add the tweets to a list, to be converted to a dataframe
        tweets_data = []
        for tweet_id in timeline:
            # collect the specific tweet data
            tweet_data = self.redis_connection.hgetall(f'tweet:{tweet_id}')

            # convert the date from iso format to normal datetime format
            tweet_data['timestamp'] = datetime.fromisoformat(tweet_data['timestamp'])

            # append to the list
            tweets_data.append(tweet_data)

        # convert the output to a dataframe, because that is what we used in Homework 1
        df = pd.DataFrame(tweets_data)

        return df

    def get_random_ID(self, csv_file):
        """
        Function to get a random user ID from the unique users in the passed Follows csv file.
        :param csv_file: The name of the follows csv file
        :return: the random id
        """

        # read in the follows content to DF
        df = pd.read_csv(csv_file)

        # make both columns into one DF
        all_ids = pd.concat([df['USER_ID'], df['FOLLOWS_ID']])

        # get only the unique ID's, to avoid double counting
        unique_ids = all_ids.unique()

        # find a random choice
        random_id = random.choice(unique_ids)

        return random_id

    def close_redis_connection(self):
        """
        Close the Redis connection
        :return: nothing
        """
        self.redis_connection.close()

    def flush_all(self):
        """
        Flush all in the redis table
        :return: nothing
        """
        self.redis_connection.flushall()