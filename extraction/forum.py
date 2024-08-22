import abc
import time
from abc import ABC
import requests
from definition import *
import os
import praw
import logging


class ForumInterface(ABC):

    @abc.abstractmethod
    def authenticate(self):
        """
        authenticate the api connection
        :return:
        """
        pass

    @abc.abstractmethod
    def make_query(self, action=GET, url="", headers=None, parameters=None):
        """
        make api request to obtain forum data
        :param action:
        :param url:
        :param headers:
        :param parameters:
        :return:
        """
        pass

    @abc.abstractmethod
    def parse_response(self, response):
        """
        parse the response to handle error code and output as a dictionary
        :param response:
        :return:
        """
        pass

    @staticmethod
    def error_handling(retries=3, delay=2):
        def decorator(func):
            def wrapper(*args, **kwargs):
                attempts = 0
                while attempts < retries:
                    try:
                        return func(*args, **kwargs)
                    except Exception as error:
                        attempts += 1
                        logging.warning(f"Attempt {attempts} failed: {error}")
                        logging.info(f"Retrying in {delay} seconds..")

            return wrapper

        return decorator


class Twitter(ForumInterface):
    """
    need to pay for basic in order to get the post
    """

    def authenticate(self):
        # token = os.getenv('Twitter_Access_Token')
        # token_secret = os.getenv('Twitter_Access_Token_Secret')
        pass

    @staticmethod
    def request_header():
        bearer_token = os.getenv("Twitter_Bearer_Token")
        return {"Authorization": f"Bearer {bearer_token}", 'Content-Type': 'application/json'}

    def make_query(self, action=GET, url="", headers=None, parameters=None):
        if headers is None:
            headers = {}
        if parameters is None:
            parameters = {}
        response = requests.request(
            action, url=url, headers=headers, params=parameters
        )

        return response

    def parse_response(self, response):
        code = response.status_code
        if code != 200:
            if 400 <= code < 500:
                raise Exception(
                    f"Cannot get data, the program will stop\n HTTP:{code} {response.text}"
                )
            return None
        return response.json()


class Reddit(ForumInterface):

    def authenticate(self):
        client_id = os.getenv('Reddit_Client_ID')
        client_secret = os.getenv('Reddit_Secret')
        user_agent = 'etl_app by /u/Wrong-Chemical7696'
        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
        return reddit

    @ForumInterface.error_handling(retries=3, delay=3)
    def make_query(self, action=GET, url="", headers=None, parameters=None):
        if parameters is None:
            parameters = {}
        reddit = self.authenticate()

        # Define the subreddit and search query
        subreddit_name = parameters['subreddit_name']  # Search across all subreddits
        query = parameters['query']
        sort = parameters['sort']
        limit = parameters['limit']
        time_filter = parameters['time_filter']

        # Fetch the latest posts about GameStop
        posts = reddit.subreddit(subreddit_name).search(query, sort=sort, limit=limit, time_filter=time_filter)
        return posts

    @ForumInterface.error_handling(retries=2, delay=0)
    def parse_response(self, response):
        result = {}
        for post in response:
            print(f"Title: {post.title}")
            print(f"Author: {post.author}")
            print(f"Created: {post.created_utc}")
            print(f"URL: {post.url}")
            print(f"Score: {post.score}")
            print(f"Comments: {post.num_comments}")
            print(f"Subreddit: {post.subreddit.display_name}")
            print("-" * 50)
            result[post.title] = {
                "Author": post.author,
                "Created": post.created_utc,
                "URL": post.url,
                "Score": post.score,
                "Comments": post.num_comments,
                "Subreddit": post.subreddit.display_name,
                "content": post.selftext,
            }
        return result


# client = Reddit()
# parameter = {
#     "sort": ["new", 'hot', 'top', 'relevance'][0],
#     "query": "GME",
#     "limit": 3,
#     "subreddit_name": "GME",
#     "time_filter": ['all', 'day', 'hour', 'month', 'week'][0],
#
# }
# fetched_post = client.make_query(parameters=parameter)
# some_post = client.parse_response(fetched_post)
# print(some_post)
