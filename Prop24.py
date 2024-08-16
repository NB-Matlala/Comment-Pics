import requests
from bs4 import BeautifulSoup
import pandas as pd
import threading
import time
from datetime import datetime
from azure.storage.blob import BlobClient

print("Buy inside code running......................")

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.5'
}

start_page = 9700
last_page = 10800
pages_per_batch = 101
thread_data = []
failed_pages = []  # List to store failed pages
tile_urls_with_fake_class = []
lock = threading.Lock()

def scrape_page(url, page):
    try:
        response = requests.get(url, headers=headers, timeout=70)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find all <style> tags with type="text/css"
            style_tags = soup.find_all('style', type='text/css')

            # Extract class names used for fake listings
            fake_class_names = []
            for style_tag in style_tags:
                style_content = style_tag.text.strip()
                for line in style_content.splitlines():
                    line = line.strip()
                    if line.startswith('.'):
                        class_name = line.split('{')[0].strip()[1:]
                        fake_class_names.append(class_name)
                        break

            p24_results = soup.find('div', class_='p24_results')
            if p24_results:
                col_9_div = p24_results.find('div', class_='col-9')
                if col_9_div:
                    tile_containers = col_9_div.find_all('div', class_='p24_tileContainer')
                    for tile in tile_containers:
                        a_tag = tile.find('a', href=True)
                        url = a_tag['href'] if a_tag else None
                        if url:
                            listing_number = "https://www.property24.com" + url
                            with lock:
                                if any(cls in tile['class'] for cls in fake_class_names):
                                    tile_urls_with_fake_class.append(listing_number)
                                else:
                                    thread_data.append(listing_number)
        else:
            with lock:
                failed_pages.append(page)
    except Exception as e:
        with lock:
            failed_pages.append(page)

def scrape_batch(start, end):
    threads = []
    for page in range(start, end + 1):
        url = f"https://www.property24.com/for-sale/advanced-search/results/p{page}?sp=pid%3d8%2c2%2c3%2c14%2c5%2c1%2c6%2c9%2c7%26so%3dNewest&PropertyCategory=House%2cApartmentOrFlat%2cTownhouse%2cVacantLandOrPlot%2cFarm%2cCommercial%2cIndustrial"
        thread = threading.Thread(target=scrape_page, args=(url, page))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()

def main():
    for start in range(start_page, last_page + 1, pages_per_batch):
        end = min(start + pages_per_batch - 1, last_page)
        scrape_batch(start, end)
        if end < last_page:
            time.sleep(60)
    
    # Retry failed pages
    if failed_pages:
        failed_pages.sort()
        print(f"Re-scraping failed pages: {failed_pages}")
        scrape_batch(min(failed_pages), max(failed_pages))
    
    global thread_data
    thread_data = list(set(thread_data))
    
    # Save data to CSV
    pd.DataFrame(thread_data).to_csv('property_listingsIDs.csv', encoding='utf-8', index=False)
    pd.DataFrame(tile_urls_with_fake_class, columns=['URLs']).to_csv('property_listingsfakeads.csv', encoding='utf-8', index=False)

def extract_property_details(listing_id):
    url = listing_id
    try:
        response = requests.get(url, headers=headers, timeout=70)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            listing_id = soup.find('div', class_='p24_listing p24_regularListing').get('data-listingnumber')
            photo_data = []
            try:
                photogrid_div = soup.find('div', class_='p24_mediaHolder hide').find('div', class_='p24_thumbnailContainer').find_all('div', class_='col-4 p24_galleryThumbnail')
                for x in photogrid_div[:8]:
                    photo_url = x.find('img').get('lazy-src')
                    photo_data.append({'Listing_ID': listing_id, 'Photo_Link': photo_url})
            except:
                print(f"No picture div found: {listing_id}")

            try:
                comment_24 = soup.find('div', class_='js_expandedText p24_expandedText hide')
                prop_desc = ' '.join(comment_24.stripped_strings)
            except:
                prop_desc = None

            current_datetime = datetime.now().strftime('%Y-%m-%d')
            data_desc = {"Listing ID": listing_id, "Description": prop_desc, "Time_stamp": current_datetime}
            return photo_data, data_desc
        else:
            return None, None
    except Exception as e:
        return None, None

def scrape_property_details(listing_ids):
    prop_desc_list = []
    photo_data_list = []

    for index, listing_id in enumerate(listing_ids, start=1):
        photo_data, prop_desc = extract_property_details(listing_id)
        if prop_desc:
            prop_desc_list.append(prop_desc)
        if photo_data:
            photo_data_list.extend(photo_data)

        if index % 200 == 0 or index == len(listing_ids):
            time.sleep(60)

    timenow = datetime.now().strftime('%H:%M')
    container_name = "comments-pics"
    filename_comments = f"Prop24Comments(2){timenow}.csv"
    filename_pics = f"Prop24pics(2){timenow}.csv"

    connection_string = "DefaultEndpointsProtocol=https;AccountName=privateproperty;AccountKey=zX/k04pby4o1V9av1a5U2E3fehg+1bo61C6cprAiPVnql+porseL1NVw6SlBBCnVaQKgxwfHjZyV+AStKg0N3A==;BlobEndpoint=https://privateproperty.blob.core.windows.net/;QueueEndpoint=https://privateproperty.queue.core.windows.net/;TableEndpoint=https://privateproperty.table.core.windows.net/;FileEndpoint=https://privateproperty.file.core.windows.net/;"
    BlobClient.from_connection_string(conn_str=connection_string, container_name=container_name, blob_name=filename_comments).upload_blob(pd.DataFrame(prop_desc_list).to_csv(encoding="utf-8", index=False), overwrite=True)
    BlobClient.from_connection_string(conn_str=connection_string, container_name=container_name, blob_name=filename_pics).upload_blob(pd.DataFrame(photo_data_list).to_csv(encoding="utf-8", index=False), overwrite=True)

if __name__ == "__main__":
    main()
    scrape_property_details(thread_data)
