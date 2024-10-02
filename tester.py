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

session = HTMLSession()

######################################## Functions ###############################################################

def worker(queue, results, pic_results):
    while True:
        item = queue.get()
        if item is None:
            break
        url = item.get("url")
        extract_function = item.get("extract_function")
        try:
            response = session.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            result = extract_function(soup)
            if result:
                if extract_function == extractor:
                    results.append(result)
                elif extract_function == extractor_pics:
                    pic_results.extend(result)
        except Exception as e:
            print(f"Request failed for {url}: {e}")
        finally:
            queue.task_done()

def getPages(soupPage, url):
    try:
        num_pg = soupPage.find('div', class_='listing-results-layout__mobile-item-count txt-small-regular')
        num_pgV = num_pg.text.split('of ')[-1]
        num_pgV = num_pgV.replace('\xa0', '').replace(' results', '')
        pages = math.ceil(int(num_pgV) / 20)
        return pages
    except (ValueError, AttributeError) as e:
        print(f"Failed to parse number of pages for URL: {url} - {e}")
        return 0

def extractor(soup):
    listing_id = soup.find('div',class_='p24_listing p24_regularListing').get('data-listingnumber')
    try:
        comment_24 = soup.find('div', class_='js_expandedText p24_expandedText hide')
        prop_desc = ' '.join(comment_24.stripped_strings)
    except:
        prop_desc = None
    current_datetime = datetime.now().strftime('%Y-%m-%d')
    data_desc= {"Listing ID": listing_id,
                "Description": prop_desc,
                "Time_stamp": current_datetime}
    return data_desc


def extractor_pics(soup): # extracts from created urls
    listing_id = soup.find('div',class_='p24_listing p24_regularListing').get('data-listingnumber')
    photo_data = []
    try:
        photogrid_div = soup.find('div',class_='p24_mediaHolder hide').find('div',class_='p24_thumbnailContainer').find_all('div',class_='col-4 p24_galleryThumbnail')
        if photogrid_div:
            counter = 0
            for x in photogrid_div:
                photo_url = x.find('img').get('lazy-src')
                photo_data.append({'Listing_ID': listing_id, 'Photo_Link': photo_url})
                counter += 1
                if counter == 8:
                    break
    except:
        print(f"No picture div found: {listing_id}")

    return photo_data

######################################## Functions ###############################################################

fieldnames = ['Listing ID', 'Description','Time_stamp']
filename = "PropGitComs2.csv"

fieldnames_pics = ['Listing_ID', 'Photo_Link']
filename_pics = "PropGitPics2.csv"

# Initialize thread queue and results list
queue = Queue()
results = []
pic_results = []
start_pg = 1
end_pg = 1000

for pg in range(start_pg, end_pg + 1):
    if pg % 20 == 0:
        time.sleep(45)
        print(f"{pg} pages passed..Sleeping...")

    response = session.get(f"https://www.property24.com/for-sale/advanced-search/results/p{pg}?sp=pid%3d8%2c2%2c3%2c14%2c5%2c1%2c6%2c9%2c7%26so%3dNewest&PropertyCategory=House%2cApartmentOrFlat%2cTownhouse%2cVacantLandOrPlot%2cFarm%2cCommercial%2cIndustrial")

    # page_content = response.text()
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all <style> tags with type="text/css"
    style_tags = soup.find_all('style', type='text/css')

    # Iterate over each style tag to find the class name after the <style type="text/css"> tag
    fake_class_names = []
    # Iterate over each style tag
    for style_tag in style_tags:
        # Extract the content of the style tag
        style_content = style_tag.text.strip()

        # Split the content by lines and iterate over lines
        for line in style_content.splitlines():
            # Strip any leading/trailing whitespace
            line = line.strip()

            # Check if the line starts with a '.' (indicating a class definition)
            if line.startswith('.'):
                # Extract the class name by splitting on '{' and taking the first part
                class_name = line.split('{')[0].strip()

                # Remove the leading '.' from the class name
                class_name = class_name[1:]  # Remove the leading '.'

                # Append the class name to the list
                fake_class_names.append(class_name)
                break

    listings_links = []

    p24_results = soup.find('div', class_='p24_results')
    if p24_results:
        col_9_div = p24_results.find('div', class_='col-9')
        if col_9_div:
            tile_containers = col_9_div.find_all('div', class_='p24_tileContainer')
            for tile in tile_containers:

                if any(cls in tile['class'] for cls in fake_class_names):
                    a_tag = tile.find('a', href=True)
                    url = a_tag['href'] if a_tag else None
                    if url is not None:
                        prop_link ="https://www.property24.com"+url

                else:
                    a_tag = tile.find('a', href=True)
                    url = a_tag['href'] if a_tag else None
                    if url is not None:
                        prop_link ="https://www.property24.com"+url
                        listings_links.append(prop_link)

    # gp_links = ['https://www.property24.com/for-sale/john-vorster-park/ermelo/mpumalanga/4853/114964552',
    #             'https://www.property24.com/for-sale/ermelo/ermelo/mpumalanga/4844/114980654']

    for list_url in listings_links:
        response_text = session.get(list_url)
        home_page = BeautifulSoup(response_text.content, 'html.parser')
        queue.put({"url": list_url, "extract_function": extractor})
        queue.put({"url": list_url, "extract_function": extractor_pics})

# Start threads
num_threads = 12
threads = []
for i in range(num_threads):
    t = threading.Thread(target=worker, args=(queue, results, pic_results))
    t.start()
    threads.append(t)

# Block until all tasks are done
queue.join()

# Stop workers
for i in range(num_threads):
    queue.put(None)
for t in threads:
    t.join()

# Write results to CSV files
with open(filename, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)

with open(filename_pics, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames_pics)
    writer.writeheader()
    writer.writerows(pic_results)

# Upload to Azure Blob Storage
blob_connection_string = "DefaultEndpointsProtocol=https;AccountName=privateproperty;AccountKey=zX/k04pby4o1V9av1a5U2E3fehg+1bo61C6cprAiPVnql+porseL1NVw6SlBBCnVaQKgxwfHjZyV+AStKg0N3A==;BlobEndpoint=https://privateproperty.blob.core.windows.net/;QueueEndpoint=https://privateproperty.queue.core.windows.net/;TableEndpoint=https://privateproperty.table.core.windows.net/;FileEndpoint=https://privateproperty.file.core.windows.net/;"

blob = BlobClient.from_connection_string(
    blob_connection_string,
    container_name="comments-pics",
    blob_name=filename
)
with open(filename, "rb") as data:
    blob.upload_blob(data, overwrite=True)

blob = BlobClient.from_connection_string(
    blob_connection_string,
    container_name="comments-pics",
    blob_name=filename_pics
)
with open(filename_pics, "rb") as data:
    blob.upload_blob(data, overwrite=True)
