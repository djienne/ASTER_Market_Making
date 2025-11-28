import requests
import time
import logging

log = logging.getLogger(__name__)

def _fetch_with_backoff(endpoint, params):
    """
    Fetches data from an API endpoint with exponential backoff for rate limiting.
    """
    backoff_time = 1  # Start with 1 second
    max_retries = 7
    for i in range(max_retries):
        try:
            response = requests.get(endpoint, params=params, timeout=20)
            
            # Successful request
            if response.status_code == 200:
                return response.json()

            # Rate limit error
            elif response.status_code == 429 or response.status_code == 418:
                log.warning(f"Rate limit hit (Status {response.status_code}). Backing off for {backoff_time}s...")
                time.sleep(backoff_time)
                backoff_time *= 2 # Exponential increase
            
            # Other client/server errors
            else:
                response.raise_for_status() # Raise an exception for other bad statuses

        except requests.exceptions.RequestException as e:
            log.error(f"Request failed: {e}. Retrying in {backoff_time}s...")
            time.sleep(backoff_time)
            backoff_time *= 2
    
    log.error("Failed to fetch data after multiple retries.")
    return None
