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
thread_data = []
session = HTMLSession()


def get_pages(base_url):
    response = session.get(base_url, timeout=70)
    soup = BeautifulSoup(response.content, 'html.parser')
    pagination_ul = soup.find('ul', class_='pagination')
    if pagination_ul:
        li_elements = pagination_ul.find_all('li')
        last_page_li = None
        for li in reversed(li_elements):
            a_tag = li.find('a')
            if a_tag:
                last_page_li = li
                break
        if last_page_li and a_tag:
            last_page_number = a_tag['data-pagenumber']
            return int(last_page_number)
    return 6500 

# property_type = None
# for key, value in url_to_property_type.items():
#     if key.split('/results')[0] in url:
#         property_type = value
#         break

pgs = get_pages('https://www.property24.com/to-rent/advanced-search/results?sp=pid%3d5%2c6') 
print(pgs,"pages found.")
for pg in range(1,6):
    
    response = session.get(f'https://www.property24.com/to-rent/advanced-search/results/p{pg}?sp=pid%3d5%2c6')
    soup = BeautifulSoup(response.content,'html.parser')

    p24_results = soup.find('div', class_='p24_results')
    if p24_results:
        col_9_div = p24_results.find('div', class_='col-9')
        if col_9_div:
            tile_containers = col_9_div.find_all('div', class_='p24_tileContainer')
            for tile in tile_containers:
                estate_agency_span = tile.find('span', class_='p24_branding')
                estate_agency = estate_agency_span.get('title', '').replace('Estate Agency profile for ', '').strip() if estate_agency_span else None
                agent_name_span = tile.find('span', class_='p24_brandingAgent')
                agent_name = agent_name_span.find('span', class_='p24_brandingAgentName').get_text(strip=True) if agent_name_span else None
                listing_number = tile.get('data-listing-number')
                a_tag = tile.find('a', href=True)
                url = a_tag['href'] if a_tag else None
                price = tile.find(class_='p24_price').get_text(strip=True) if tile.find(class_='p24_price') else None
                title = tile.find(class_='p24_description').get_text(strip=True) if tile.find(class_='p24_description') else tile.find('span', class_='p24_title').get_text(strip=True) if tile.find('span', class_='p24_title') else None
                location = tile.find('span', class_='p24_location').get_text(strip=True) if tile.find('span', class_='p24_location') else None
                address = tile.find('span', class_='p24_address').get_text(strip=True) if tile.find('span', class_='p24_address') else None
                bedrooms = tile.find('span', class_='p24_featureDetails', title='Bedrooms').find('span').get_text(strip=True) if tile.find('span', class_='p24_featureDetails', title='Bedrooms') else None
                bathrooms = tile.find('span', class_='p24_featureDetails', title='Bathrooms').find('span').get_text(strip=True) if tile.find('span', class_='p24_featureDetails', title='Bathrooms') else None
                parking_spaces = tile.find('span', class_='p24_featureDetails', title='Parking Spaces').find('span').get_text(strip=True) if tile.find('span', class_='p24_featureDetails', title='Parking Spaces') else None
                erf_size = tile.find('span', class_='p24_size', title='Erf Size').find('span').get_text(strip=True) if tile.find('span', class_='p24_size', title='Erf Size') else None
                if erf_size is None:
                    img_element = tile.find(class_='p24_sizeIcon')
                    if img_element:
                        erf_size_element = img_element.find_next_sibling('span')
                        erf_size = erf_size_element.text.strip() if erf_size_element else None
                floor_size = tile.find('span', class_='p24_size', title='Floor Size').find('span').get_text(strip=True) if tile.find('span', class_='p24_size', title='Floor Size') else None
                Timestamp = datetime.now().strftime('%Y-%m-%d')
                car_data = {'title': title, 'listing_number': listing_number, 'price': price, 'estate_agency': estate_agency, 'agent_name': agent_name,
                            'location': location, 'address': address, 'bedrooms': bedrooms, 'bathrooms': bathrooms, 'parking_spaces': parking_spaces,
                            'erf_size': erf_size, 'floor_size': floor_size, 'url': url,'Timestamp':Timestamp}
                print(car_data)
                thread_data.append(car_data)
    # print(pg,"scraped.")        
# for d in thread_data:
#     print(d)
# print(len(thread_data)," listings found.")
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
