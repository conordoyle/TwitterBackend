
class Tweet:

    def __init__(self, user_id, tweet_text):
        """
        :param user_id: the user id of the tweet
        :param tweet_text: the text of the tweet
        """
        self.user_id = user_id
        self.text = tweet_text