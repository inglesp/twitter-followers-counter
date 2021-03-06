# Twitter Follower Counter

This is an example application to demonstrate building a simple data pipeline for gathering, processing, and presenting data.

The application retreives a list of all of your Twitter followers and produces a report showing how many followers you have, and who has followed and unfollowed you today.

The application depends on `requests` and `requests-oauthlib`, which can be installed by running `pip install -r requirements.txt`.

To run, you will need to follow [these instructions](https://dev.twitter.com/oauth/overview/application-owner-access-tokens) to create a Twitter app and generate an access token.  You will then need to update `credentials.py` with your app's credentials.

You should then schedule `python twitter_follower_counter.py` to be run once a day.
