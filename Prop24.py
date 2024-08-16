
print("Buy inside code running......................")

import aiohttp
import asyncio
from bs4 import BeautifulSoup
import datetime
import pandas as pd
import requests
from azure.storage.blob import BlobClient
from datetime import datetime, timedelta
import time

from datetime import datetime
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


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

async def scrape_page(session, url, page):
    try:
        async with session.get(url, headers=headers, timeout=70) as response:
            if response.status == 200:
                page_content = await response.text()
                soup = BeautifulSoup(page_content, 'html.parser')

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

                # Print out the extracted fake class names
                #print("Fake Class Names Found:")
                #for class_name in fake_class_names:
                #    print(class_name)


                p24_results = soup.find('div', class_='p24_results')
                if p24_results:
                    col_9_div = p24_results.find('div', class_='col-9')
                    #if col_9_div:
                    #    tile_containers = col_9_div.find_all('div', class_='p24_tileContainer')
                    #    for tile in tile_containers:
                    #        listing_number = tile.get('data-listing-number')
                    #        if listing_number:
                    #            listing_number = listing_number.replace('P', '')
                    #        thread_data.append(listing_number)

                            #-----------------------------------------
                    if col_9_div:
                        tile_containers = col_9_div.find_all('div', class_='p24_tileContainer')
                        for tile in tile_containers:

                            if any(cls in tile['class'] for cls in fake_class_names):
                                a_tag = tile.find('a', href=True)
                                url = a_tag['href'] if a_tag else None
                                if url is not None:
                                    listing_number ="https://www.property24.com"+url
                                    #print("urls:",listing_number)
                                    tile_urls_with_fake_class.append(listing_number)
                                    #print("fake url",tile_urls_with_fake_class)

                            else:
                                a_tag = tile.find('a', href=True)
                                url = a_tag['href'] if a_tag else None
                                if url is not None:
                                    listing_number ="https://www.property24.com"+url
                                    #print("urls:",listing_number)
                                    thread_data.append(listing_number)

            else:
                #print(f"Failed to retrieve page {page}: Status code {response.status}")
                failed_pages.append(page)
    except Exception as e:
        #print(f"An error occurred while scraping page {page}: {e}")
        failed_pages.append(page)

async def scrape_batch(session, start, end):
    tasks = []
    for page in range(start, end + 1):
        url = f"https://www.property24.com/for-sale/advanced-search/results/p{page}?sp=pid%3d8%2c2%2c3%2c14%2c5%2c1%2c6%2c9%2c7%26so%3dNewest&PropertyCategory=House%2cApartmentOrFlat%2cTownhouse%2cVacantLandOrPlot%2cFarm%2cCommercial%2cIndustrial"
        tasks.append(scrape_page(session, url, page))
    await asyncio.gather(*tasks)

async def main():
    async with aiohttp.ClientSession() as session:
        for start in range(start_page, last_page + 1, pages_per_batch):
            end = min(start + pages_per_batch - 1, last_page)
            await scrape_batch(session, start, end)
            if end < last_page:
                #print(f"Sleeping for 60 seconds before scraping pages {end + 1} to {min(end + pages_per_batch, last_page)}")
                await asyncio.sleep(60)
        global failed_pages
        # Scrape failed pages as a new batch
        if failed_pages:
            failed_pages = list(set(failed_pages))
            #print(f"Re-scraping failed pages: {failed_pages}")
            await scrape_batch(session, min(failed_pages), max(failed_pages))

if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    thread_data = [item for item in thread_data if item is not None]
    thread_data = list(set(thread_data))
    #print(thread_data)
    #print(len(thread_data))
    data_frame = pd.DataFrame(thread_data)
    data_frame.to_csv('property_listingsIDs.csv', encoding='utf-8', index=False)

    data_frame2 = pd.DataFrame(tile_urls_with_fake_class, columns=['URLs'])
    data_frame2.to_csv('property_listingsfakeads.csv', encoding='utf-8', index=False)

time.sleep(30)

import aiohttp
import asyncio
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import time

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.5'
}
base_url = "https://www.property24.com/for-sale/uvongo/margate/kwazulu-natal/6359/{id}"
def get_text_or_none(element):
    return element.get_text(strip=True) if element else None

async def extract_property_details(session, listing_id):
    url = listing_id
    try:
        async with session.get(url, timeout=70) as response:
            if response.status != 200:
                print(f"respomse status:{response.status}")

            if response.status == 200:
                page_content = await response.text()
                soup = BeautifulSoup(page_content, 'html.parser')

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

                return photo_data,data_desc
            else:
                return None
    except Exception as e:
        return None

async def scrape_property_details(listing_ids):
    prop_desc_list=[]
    photo_data_list=[]

    async with aiohttp.ClientSession() as session:
        for index, listing_id in enumerate(listing_ids, start=1):
            try:
                #property_details = await extract_property_details(session, listing_id)
                photo_data,prop_desc = await extract_property_details(session, listing_id)
                if prop_desc is not None:
                    prop_desc_list.append(prop_desc)

                if photo_data is not None:
                    photo_data_list.extend(photo_data)

            except Exception as e:
                pass

            if index % 200 == 0 or index == len(listing_ids):
                await asyncio.sleep(60)


    import time
    container_name = "comments-pics"
    timenow= datetime.now().strftime('%H:%M')
    filename_comments = f"Prop24Comments(2){timenow}.csv"
    filename_pics = f"Prop24pics(2){timenow}.csv"

    data_frame_comments = pd.DataFrame(prop_desc_list)
    data_frame_photo = pd.DataFrame(photo_data_list)
    # Save DataFrame to CSV
    car_data_csv_com = data_frame_comments.to_csv(encoding="utf-8", index=False)
    car_data_csv_pic = data_frame_photo.to_csv(encoding="utf-8", index=False)
    connection_string = "DefaultEndpointsProtocol=https;AccountName=privateproperty;AccountKey=zX/k04pby4o1V9av1a5U2E3fehg+1bo61C6cprAiPVnql+porseL1NVw6SlBBCnVaQKgxwfHjZyV+AStKg0N3A==;BlobEndpoint=https://privateproperty.blob.core.windows.net/;QueueEndpoint=https://privateproperty.queue.core.windows.net/;TableEndpoint=https://privateproperty.table.core.windows.net/;FileEndpoint=https://privateproperty.file.core.windows.net/;"
    client = BlobClient.from_connection_string(conn_str=connection_string, container_name=container_name, blob_name=filename_comments)
    client.upload_blob(car_data_csv_com, overwrite=True)
    client = BlobClient.from_connection_string(conn_str=connection_string, container_name=container_name, blob_name=filename_pics)
    client.upload_blob(car_data_csv_pic, overwrite=True)


async def main():
    global thread_data

    # Scrape property details asynchronously
    await scrape_property_details(thread_data)

if __name__ == "__main__":
    asyncio.run(main())
