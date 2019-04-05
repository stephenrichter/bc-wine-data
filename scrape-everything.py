from bs4 import BeautifulSoup
import os
import shutil
import time
import requests
import re
import json
import glob

BASE_URL = 'https://www.everythingwine.ca/rose?p={0}'
HEADERS = {
    'user-agent': ('Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36')
}
DATA_DIR = 'data'
FILENAME = 'everything-wine_data'

class Scraper():
    """Scraper for EverythingWine.ca to collect wine data"""

    def __init__(self, pages_to_scrape=(1,1), clear_old_data=True):
      self.pages_to_scrape = pages_to_scrape
      self.clear_old_data = clear_old_data
      self.session = requests.Session()
      # Set variables for the scrape status timing function
      self.start_time = time.time()
      self.cross_process_product_count = 0
      self.estimated_total_products = (pages_to_scrape[1] + 1 - pages_to_scrape[0]) * 8
    
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
      
      soup = BeautifulSoup(response.content,'lxml')
      products = soup.find_all('li', {'class': 'item'})
      for product in products:
        self.cross_process_product_count += 1
        product_url = product.find('a')['href']

        if product.find("span", {"class": "regular-price"}) is not None:
          product_price = product.find("span", {"class": "regular-price"}).find("span", {"class": "price"}).text
        elif product.find("span", {"class": "special-price"}) is not None:
          product_price = product.find("span", {"class": "special-price"}).find("span", {"class": "price"}).text
        else:
          product_price = None
        
        try:
          product_data = self.scrape_product(product_url)
          product_data['price'] = product_price
        except Exception as e:
          print('Encountered error', e)
          continue
        scrape_data.append(product_data)
        self.update_scrape_status()
      self.save_data(scrape_data)
    
    def scrape_product(self, product_url):
      product_response = self.session.get(product_url, headers=HEADERS)
      product_soup = BeautifulSoup(product_response.content,'lxml')
      try:
        return self.parse_product(product_soup, product_url)
      except ReviewFormatException as e:
        print('\n-----\nError parsing: {}\n{}\n-----'.format(
          product_url,
          e
        ))
    
    def parse_product(self, product_soup, product_url):
      name = product_soup.find("div", {"class", "product-name"}).findChildren("h1")[0].text
      summary = product_soup.find("div", {"class", "product-summary"}).findChildren("p")[0].text
      sku = product_soup.find("div", {"class", "product-sku"}).contents[0].split(': ')[1]

      try:
        attributes_soup = product_soup.findAll("div", {"class", "attribute"})
        attributes = []
        for attribute in attributes_soup:
          attributes.append(attribute.findChildren("p")[0])
        if attributes[0] is not None:
          country = attributes[0].text
        else:
          country = None
        if attributes[1] is not None:
          region = attributes[1].text
        else:
          region = None
        if attributes[2] is not None:
          style = attributes[2].text
        else:
          style= None
        if attributes[3] is not None:
          grape = attributes[3].text
        else:
          grape = None
      except:
        raise ReviewFormatException('Problem with attributes')

      # Try and get any tasting notes that exist on the page
      try:
        tasting_notes = product_soup.find("li", {"class": "tasting_notes"}).find("div", {"class", "value"}).text.strip()
      except:
        tasting_notes = None
      
      # Try and get any food pairings that exist on the page
      try:
        food_pairings = product_soup.find("li", {"class": "food_pairings"}).find("div", {"class", "value"}).text.strip()
      except:
        food_pairings = None
      
      # Try and get any ratings that exist on the page
      try:
        rating_soup = product_soup\
                      .find("div", {"class", "extra-info"})\
                      .find("div", {"class", "rating"})['style']
        rating = rating_soup.split(':')[1]
      except:
        rating = None
      
      product_data = {
        'name': name,
        'summary': summary,
        'sku': sku,
        'url': product_url,
        'country': country,
        'region': region,
        'style': style,
        'grape': grape,
        'rating': rating,
        'tasting_notes': tasting_notes,
        'food_pairings': food_pairings
      }
      return product_data

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

class ReviewFormatException(Exception):
    """Exception when the format of a review page is not understood by the scraper"""
    def __init__(self, message):
        self.message = message
        super(Exception, self).__init__(message)

if __name__ == '__main__':
  pages_to_scrape = (1,13) # Set the page number to start, and total pages to scrape
  wine_scraper = Scraper(pages_to_scrape=pages_to_scrape)

  # Step 1: scrape data
  wine_scraper.scrape_site()

  # Step 2: condense data
  wine_scraper.condense_data()