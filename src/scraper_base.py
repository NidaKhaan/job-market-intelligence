import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime
import random

class BaseScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.session = requests.Session()
    
    def respectful_request(self, url, delay=2):
        """Makes request with delay to be respectful"""
        time.sleep(delay + random.uniform(0, 1))
        try:
            response = self.session.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response
        except Exception as e:
            print(f"❌ Error fetching {url}: {e}")
            return None
    
    def parse_jobs(self, html):
        """Override this in specific scrapers"""
        raise NotImplementedError("Subclass must implement parse_jobs()")

if __name__ == "__main__":
    print("✅ Base scraper initialized")