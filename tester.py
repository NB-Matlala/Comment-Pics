from bs4 import BeautifulSoup
from requests_html import HTMLSession
import threading
from queue import Queue
from datetime import datetime
import csv
from azure.storage.blob import BlobClient
import time

session = HTMLSession()

######################################## Functions ###############################################################

def worker(queue, results, pic_results):
    while True:
        item = queue.get()
        if item is None:
            break
        url = item.get("url")
        try:
            response = session.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            result = extractor(soup)
            pics = extractor_pics(soup)
            if result:
                results.append(result)
            if pics:
                pic_results.extend(pics)
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
    try:
        listing_id = soup.find('div',class_='p24_listing p24_regularListing').get('data-listingnumber')
        comment_24 = soup.find('div', class_='js_expandedText p24_expandedText hide')
        prop_desc = ' '.join(comment_24.stripped_strings) if comment_24 else None
        current_datetime = datetime.now().strftime('%Y-%m-%d')
        return {"Listing ID": listing_id, "Description": prop_desc, "Time_stamp": current_datetime}
    except Exception as e:
        print(f"Extraction failed for id {listing_id} Error: {e}")
        return None

def extractor_pics(soup):
    try:
        listing_id = soup.find('div',class_='p24_listing p24_regularListing').get('data-listingnumber')
        photo_data = []
        photogrid_div = soup.find('div',class_='p24_mediaHolder hide').find('div',class_='p24_thumbnailContainer').find_all('div',class_='col-4 p24_galleryThumbnail')
        for counter, x in enumerate(photogrid_div, start=1):
            photo_url = x.find('img').get('lazy-src')
            photo_data.append({'Listing_ID': listing_id, 'Photo_Link': photo_url})
            if counter == 8:  # Limit to 8 images
                break
        return photo_data
    except Exception as e:
        print(f"Photo extraction failed for id {listing_id} Error: {e}")
        return []

######################################## Main Code ###############################################################

fieldnames = ['Listing ID', 'Description','Time_stamp']
filename = "PropGitComs2.csv"
fieldnames_pics = ['Listing_ID', 'Photo_Link']
filename_pics = "PropGitPics2.csv"

# Initialize thread queue and results list
queue = Queue()
results = []
pic_results = []
start_pg = 1
end_pg = 30

for pg in range(start_pg, end_pg + 1):
    if pg % 20 == 0:
        time.sleep(60)
        print(f"{pg} pages passed..Sleeping...")
    
    response = session.get(f"https://www.property24.com/for-sale/advanced-search/results/p{pg}?sp=pid%3d8%2c2%2c3%2c14%2c5%2c1%2c6%2c9%2c7%26so%3dNewest&PropertyCategory=House%2cApartmentOrFlat%2cTownhouse%2cVacantLandOrPlot%2cFarm%2cCommercial%2cIndustrial")
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Fake class names handling
    style_tags = soup.find_all('style', type='text/css')
    fake_class_names = [line.split('{')[0].strip()[1:] for style_tag in style_tags for line in style_tag.text.splitlines() if line.startswith('.')]
    
    listings_links = []
    p24_results = soup.find('div', class_='p24_results')
    if p24_results:
        col_9_div = p24_results.find('div', class_='col-9')
        if col_9_div:
            tile_containers = col_9_div.find_all('div', class_='p24_tileContainer')
            for tile in tile_containers:
                a_tag = tile.find('a', href=True)
                url = a_tag['href'] if a_tag else None
                if url:
                    prop_link = "https://www.property24.com" + url
                    listings_links.append(prop_link)

    for list_url in listings_links:
        queue.put({"url": list_url})

# Start threads
num_threads = 10
threads = []
for i in range(num_threads):
    t = threading.Thread(target=worker, args=(queue, results, pic_results))
    t.start()
    threads.append(t)

# Block until all tasks are done
queue.join()

# Stop workers
for t in threads:
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
