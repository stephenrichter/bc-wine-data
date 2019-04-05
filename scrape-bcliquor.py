import os
import shutil
import time
import requests
import re
import json
import glob

BASE_URL = 'http://www.bcliquorstores.com/ajax/browse?sort=name.raw:asc&category=wine&size=100&page={0}'
HEADERS = {
    'user-agent': ('Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36')
}
DATA_DIR = 'bcl_data'
FILENAME = 'bcliquor_data'

class Scraper():
    """Scraper for BCLiquorStores.com to collect wine data"""

    def __init__(self, pages_to_scrape=(1,1), clear_old_data=True):
      self.pages_to_scrape = pages_to_scrape
      self.clear_old_data = clear_old_data
      self.session = requests.Session()
      # Set variables for the scrape status timing function
      self.start_time = time.time()
      self.cross_process_product_count = 0
      self.estimated_total_products = (pages_to_scrape[1] + 1 - pages_to_scrape[0]) * 100
    
    def scrape_site(self):
      if self.clear_old_data:
        self.clear_data_dir()

      for page in range(self.pages_to_scrape[0], self.pages_to_scrape[1] + 1):
        self.scrape_page(BASE_URL.format(page))

      print('Scrape finished...')
    
    def scrape_page(self, page_url, retry_count=0):
      scrape_data = []
      try:
        response = self.session.get(page_url, headers=HEADERS)
      except:
        retry_count += 1
        if retry_count <= 3:
          self.session = requests.Session()
          self.scrape_page(page_url, retry_count)
        else:
          raise
      
      response_data = response.json()
      products = response_data['hits']['hits']
      for product in products:
        self.cross_process_product_count += 1
        scrape_data.append(product['_source'])
        self.update_scrape_status()
      self.save_data(scrape_data)

    def save_data(self, data):
      filename = '{}/{}_{}.json'.format(DATA_DIR, FILENAME, time.time())
      try:
        os.makedirs(DATA_DIR)
      except OSError:
        pass
      with open(filename, 'w') as fout:
        json.dump(data, fout)
    
    def clear_all_data(self):
        self.clear_data_dir()
        self.clear_output_data()

    def clear_data_dir(self):
        try:
            shutil.rmtree(DATA_DIR)
        except FileNotFoundError:
            pass

    def clear_output_data(self):
        try:
            os.remove('{}.json'.format(FILENAME))
        except FileNotFoundError:
            pass

    def condense_data(self):
        print('Condensing Data...')
        condensed_data = []
        all_files = glob.glob('{}/*.json'.format(DATA_DIR))
        for file in all_files:
            with open(file, 'rb') as fin:
                condensed_data += json.load(fin)
        print(len(condensed_data))
        filename = '{}.json'.format(FILENAME)
        with open(filename, 'w') as fout:
            json.dump(condensed_data, fout)
    
    def update_scrape_status(self):
        elapsed_time = round(time.time() - self.start_time, 2)
        time_remaining = round((self.estimated_total_products - self.cross_process_product_count) * (self.cross_process_product_count / elapsed_time), 2)
        print('{0}/{1} products | {2} sec elapsed | {3} sec remaining\r'.format(
            self.cross_process_product_count, self.estimated_total_products, elapsed_time, time_remaining))

if __name__ == '__main__':
  pages_to_scrape = (1,39) # Set the page number to start, and total pages to scrape
  wine_scraper = Scraper(pages_to_scrape=pages_to_scrape)

  # Step 1: scrape data
  wine_scraper.scrape_site()

  # Step 2: condense data
  wine_scraper.condense_data()