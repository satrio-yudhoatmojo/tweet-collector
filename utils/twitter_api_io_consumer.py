import logging
import os

import ndjson
import requests

from utils import twitter_api_io_producer
from helpers.scheduler import Scheduler


class TwitterApiIOConsumer:
    BASE_URL = 'https://api.twitterapi.io/twitter'

    def __init__(self, api_key: str):
        self.api_key = os.getenv("TWITTER_API_IO_KEY") 
        self.headers = {'x-api-key': self.api_key}
        self.twitter_api_io_producer = twitter_api_io_producer.TwitterApiIOProducer()
        self.logger = logging.getLogger(__name__)


    def advanced_search(self, query: str, query_type: str = 'Latest', next_cursor: str = None, output_filepath: str = "output/search_results.ndjson"):
        url = f'{self.BASE_URL}/tweet/advanced_search'

        params = {
            'query': query,
            'queryType': query_type,
        }
        if next_cursor:
            params['next_cursor'] = next_cursor

        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()

            if response.status_code == 200:
                self.logger.info('Advanced search successful.')

                response_data = response.json()
                if 'tweets' in response_data:
                    if 'next_cursor' in response_data:
                        minute_to_start = 1 # Delay by 1 minute to avoid rate limits
                        at = Scheduler.get_scheduled_time(minute_to_start)

                        self.twitter_api_io_producer.enqueue_search_job(query=query, query_type=query_type, next_cursor=response_data['next_cursor'], start_time=at, output_filepath=output_filepath)
                    
                    else:
                        self.logger.info('No next_cursor found; not enqueuing further search jobs.')
                    
                    response_data['collected_at'] = str(requests.utils.formatdate(usegmt=True))
                    self._save_search_results_ndjson(response_data['tweets'], output_filepath)  
                
                else:
                    self.logger.info('No tweets found in the response.')

            else:
                self.logger.error(f'Advanced search failed with status code: {response.status_code}')
        
        except requests.RequestException as e:
            self.logger.error(f'Error during advanced search: {e}')


    def _save_search_results_ndjson(self, data, file_path: str):
        self.logger.info(f'Saving search results to {file_path}')

        if os.path.isfile(file_path):
            with open(file_path, 'a') as f:
                writer = ndjson.writer(f, ensure_ascii=False)
                writer.writerow(data)
        else:
            with open(file_path, 'w') as f:
                writer = ndjson.writer(f, ensure_ascii=False)
                for item in data:
                    writer.writerow(item)

        self.logger.info(f'Search results saved to {file_path}')