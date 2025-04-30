import os
import json
import tweepy
import logging
from azure.eventhub import EventHubProducerClient, EventData
import azure.functions as func

def main(mytimer: func.TimerRequest) -> None:
    logging.info("TwitterIngestFunction triggered using Twitter API v2")

    # Twitter API v2 bearer token
    bearer_token = os.environ["TWITTER_BEARER_TOKEN"]

    # Azure Event Hub connection
    eventhub_conn_str = os.environ["EVENTHUB_CONN_STR"]
    eventhub_name = os.environ["EVENT_HUB_NAME"]
    
    try:
        # Initialize Twitter client
        client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)

        queries = [
            # Liberal-related
            """("#CanadaElectionS2025" OR "liberal party" OR "liberals" OR "lpc" OR "justin trudeau" OR "trudeau" OR "mark carney" OR "carney" OR "#teamtrudeau" OR "#trudeaumustgo") -is:retweet -is:reply -is:quote lang:en""",

            # Conservative-related
            """("conservative party" OR "conservatives" OR "cpc" OR "pierre poilievre" OR "poilievre" OR 
            OR "#pierrepoilievre") -is:retweet -is:reply -is:quote lang:en"""
        ]

        # Initialize Event Hub producer
        producer = EventHubProducerClient.from_connection_string(
            conn_str=eventhub_conn_str,
            eventhub_name=eventhub_name
        )

        # Create a batch to hold EventData
        event_data_batch = producer.create_batch()

        # Fetch and send tweets for each query
        for query in queries:
            logging.info(f"Searching tweets for query: {query}")
            response = client.search_recent_tweets(
                query=query,
                max_results=100,
                tweet_fields=["created_at", "author_id"]
            )

            if response.data:
                # Create map of user_id to user info (for locations)
                user_dict = {}
                if "users" in response.includes:
                    for user in response.includes["users"]:
                        user_dict[user.id] = user

                # Create a batch
                event_data_batch = producer.create_batch()

                for tweet in response.data:
                    user = user_dict.get(tweet.author_id)
                    location = user.location if user and user.location else "Unknown"

                    data = {
                        "id": tweet.id,
                        "text": tweet.text,
                        "author_id": tweet.author_id,
                        "created_at": str(tweet.created_at)
                    }
                    json_data = json.dumps(data)
                    logging.info(f"Tweet JSON: {json_data}")

                    try:
                        # Add each tweet to batch
                        event_data_batch.add(EventData(json_data))
                    except ValueError:
                        # If the batch is full, send it and create a new batch
                        producer.send_batch(event_data_batch)
                        logging.info("Event batch full. Sending batch to Event Hub...")
                        event_data_batch = producer.create_batch()
                        event_data_batch.add(EventData(json_data))
            else:
                logging.info("No tweets found for this query.")

        # Send remaining tweets if any
        if len(event_data_batch) > 0:
            producer.send_batch(event_data_batch)
            logging.info("Sent final batch to Event Hub.")

    except Exception as e:
        logging.error(f"Error occurred: {e}", exc_info=True)

