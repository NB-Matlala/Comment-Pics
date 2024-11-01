from bs4 import BeautifulSoup
from requests_html import HTMLSession
import re
import math
import json
import time
import threading
from queue import Queue
from datetime import datetime
import csv
from azure.storage.blob import BlobClient
import os

# base_url = os.getenv("BASE_URL")
# con_str_coms = os.getenv("CON_STR_COMS")
session = HTMLSession()

# # Thread worker function
# def worker(queue, results, pic_results):
#     while True:
#         item = queue.get()
#         if item is None:
#             break
#         url = item.get("url")
#         extract_function = item.get("extract_function")
#         try:
#             response = session.get(url)
#             soup = BeautifulSoup(response.content, 'html.parser')
#             result = extract_function(soup, url)
#             if result:
#                 if extract_function == extractor:
#                     results.append(result)
#                 elif extract_function == extractor_pics:
#                     pic_results.extend(result)
#         except Exception as e:
#             print(f"Request failed for {url}: {e}")
#         finally:
#             queue.task_done()

# def getPages(soupPage, url):
#     try:
#         num_pg = soupPage.find('div', class_='listing-results-layout__mobile-item-count txt-small-regular')
#         num_pgV = num_pg.text.split('of ')[-1]
#         num_pgV = num_pgV.replace('\xa0', '').replace(' results', '')
#         pages = math.ceil(int(num_pgV) / 20)
#         return pages
#     except (ValueError, AttributeError) as e:
#         print(f"Failed to parse number of pages for URL: {url} - {e}")
#         return 0

# def extractor(soup, url):
#     try:
#         prop_ID = None
#         prop_div = soup.find('div', class_='property-features')
#         lists = prop_div.find('ul', class_='property-features__list')
#         features = lists.find_all('li')
#         for feature in features:
#             icon = feature.find('svg').find('use').get('xlink:href')
#             if '#listing-alt' in icon:
#                 prop_ID = feature.find('span', class_='property-features__value').text.strip()
#     except KeyError:
#         prop_ID = None

#     try:
#         comment_div = soup.find('div', class_='listing-description__text')
#         prop_desc = comment_div.text.strip()
#     except:
#         prop_desc = None

#     current_datetime = datetime.now().strftime('%Y-%m-%d')

#     return {
#         "Listing ID": prop_ID, "Description": prop_desc, "Time_stamp": current_datetime}

# def extractor_pics(soup, prop_id): # extracts from created urls
#     try:
#         prop_ID = None
#         prop_div = soup.find('div', class_='property-features')
#         lists = prop_div.find('ul', class_='property-features__list')
#         features = lists.find_all('li')
#         for feature in features:
#             icon = feature.find('svg').find('use').get('xlink:href')
#             if '#listing-alt' in icon:
#                 prop_ID = feature.find('span', class_='property-features__value').text.strip()
#     except KeyError:
#         prop_ID = None
#     list_id = prop_ID
    
#     try:
#         photo_div = soup.find('div', class_='details-page-photogrid__photos')
#         photo_data = []
#         img_links = photo_div.find_all('img')
#         count = 0
#         for url in img_links:
#             count += 1
#             photo_data.append({'Listing_ID': list_id, 'Photo_Link': url.get('src')})
#             if count == 8:
#                 break
#         return photo_data        
#     except KeyError:
#         print('Pictures not found')
#         return []

# def getIds(soup):
#     try:
#         script_data = soup.find('script', type='application/ld+json').string
#         json_data = json.loads(script_data)
#         url = json_data['url']
#         prop_ID_match = re.search(r'/([^/]+)$', url)
#         if prop_ID_match:
#             return prop_ID_match.group(1)
#     except Exception as e:
#         print(f"Error extracting ID from {soup}: {e}")
#     return None

# fieldnames = ['Listing ID', 'Description', 'Time_stamp']
# filename = "PrivComments2.csv"

# fieldnames_pics = ['Listing_ID', 'Photo_Link']
# filename_pics = "PrivPictures2.csv"

# # Initialize thread queue and results list
# queue = Queue()
# results = []
# pic_results = []

# response_text = session.get(f"{base_url}/for-sale/mpumalanga/4")
# home_page = BeautifulSoup(response_text.content, 'html.parser')

# links = []
# ul = home_page.find('ul', class_='region-content-holder__unordered-list')
# li_items = ul.find_all('li')
# for area in li_items:
#     link = area.find('a')
#     link = f"{base_url}{link.get('href')}"
#     links.append(link)

# new_links = []
# for l in links:
#     try:
#         res_in_text = session.get(f"{l}")
#         inner = BeautifulSoup(res_in_text.content, 'html.parser')
#         ul2 = inner.find('ul', class_='region-content-holder__unordered-list')
#         if ul2:
#             li_items2 = ul2.find_all('li', class_='region-content-holder__list')
#             for area2 in li_items2:
#                 link2 = area2.find('a')
#                 link2 = f"{base_url}{link2.get('href')}"
#                 new_links.append(link2)
#         else:
#             new_links.append(l)
#     except Exception as e:
#         print(f"Request failed for {l}: {e}")

# for x in new_links:
#     try:
#         land = session.get(x)
#         land_html = BeautifulSoup(land.content, 'html.parser')
#         pgs = getPages(land_html, x)
#         for p in range(1, pgs + 1):
#             home_page = session.get(f"{x}?page={p}")
#             soup = BeautifulSoup(home_page.content, 'html.parser')
#             prop_contain = soup.find_all('a', class_='listing-result')
#             for x_page in prop_contain:
#                 prop_id = getIds(x_page)
#                 if prop_id:
#                     list_url = f"{base_url}/for-sale/something/something/something/{prop_id}"
#                     queue.put({"url": list_url, "extract_function": extractor})
#                     queue.put({"url": list_url, "extract_function": extractor_pics})
#     except Exception as e:
#         print(f"Failed to process URL {x}: {e}")

# # Start threads
# num_threads = 15  
# threads = []
# for i in range(num_threads):
#     t = threading.Thread(target=worker, args=(queue, results, pic_results))
#     t.start()
#     threads.append(t)

# # Block until all tasks are done
# queue.join()

# # Stop workers
# for i in range(num_threads):
#     queue.put(None)
# for t in threads:
#     t.join()

# # Write results to CSV files
# with open(filename, mode='w', newline='', encoding='utf-8') as file:
#     writer = csv.DictWriter(file, fieldnames=fieldnames)
#     writer.writeheader()
#     writer.writerows(results)

# with open(filename_pics, mode='w', newline='', encoding='utf-8') as file:
#     writer = csv.DictWriter(file, fieldnames=fieldnames_pics)
#     writer.writeheader()
#     writer.writerows(pic_results)

# # Upload to Azure Blob Storage
# blob_connection_string = f"{con_str_coms}"
# blob = BlobClient.from_connection_string(
#     blob_connection_string,
#     container_name="privateprop",
#     blob_name=filename
# )
# with open(filename, "rb") as data:
#     blob.upload_blob(data, overwrite=True)

# blob_pics = BlobClient.from_connection_string(
#     blob_connection_string,
#     container_name="privateprop",
#     blob_name=filename_pics
# )
# with open(filename_pics, "rb") as data:
#     blob_pics.upload_blob(data, overwrite=True)

print("CSV files uploaded to Azure Blob Storage.")
