import argparse
import logging

from utils import twitter_api_io_consumer 
from utils import twitter_api_io_producer


if __name__ == "__main__":
    taioc = twitter_api_io_consumer.TwitterApiIOConsumer()
    taiop = twitter_api_io_producer.TwitterApiIOProducer()

    parser = argparse.ArgumentParser(description="Search Tweets using Twitter API IO")
    parser.add_argument("--query", type=str, required=True, help="Search query")
    parser.add_argument("--query_type", type=str, default="Latest", help="Type of query (e.g., Latest, Popular)")
    parser.add_argument("--next_cursor", type=str, help="Next cursor for pagination")
    parser.add_argument("--start_time", type=str, help="Start time for scheduling the job (format: YYYY-MM-DD_HH:MM:SS)")   
    parser.add_argument("--output_filepath", type=str, default="output/search_results.ndjson", help="Output file path for NDJSON results")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    taiop.enqueue_search_job(query=args.query, query_type=args.query_type, next_cursor=args.next_cursor, start_time=args.start_time, output_filepath=args.output_filepath)
