import datetime
import logging

import faktory


class TwitterApiIOProducer:
    def __init__(self):
        pass

    def enqueue_search_job(self, query=None, query_type=None, next_cursor=None, start_time=None, output_filepath="output/search_results.ndjson"):
        if not start_time:
            start_time = datetime.datetime.now().isoformat()
            
        with faktory.connection() as client:
            params = [query, query_type, next_cursor, output_filepath]
            logging.info(f"Enqueuing TwitterApiIO search job with start_time: {start_time}")
            client.queue("advanced_search", args=(params), at=start_time)
