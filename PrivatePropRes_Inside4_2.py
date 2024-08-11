import aiohttp
import asyncio
import re
from bs4 import BeautifulSoup
import json
import random
import csv
import math
from datetime import datetime
from azure.storage.blob import BlobClient

async def fetch(session, url, semaphore):
    async with semaphore:
        async with session.get(url) as response:
            return await response.text()

######################################Functions##########################################################
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

def getIds(soup):
    script_data = soup.find('script', type='application/ld+json').string
    json_data = json.loads(script_data)
    try:
        url = json_data['url']
        prop_ID_match = re.search(r'/([^/]+)$', url)
        if prop_ID_match:
            prop_ID = prop_ID_match.group(1)
        else:
            prop_ID = None
    except KeyError:
        prop_ID = None

    return prop_ID

def extractor(soup, url): # extracts from created urls
    try:
        prop_ID = None
        prop_div = soup.find('div', class_='property-features')
        lists = prop_div.find('ul', class_='property-features__list')
        features = lists.find_all('li')
        for feature in features:
            icon = feature.find('svg').find('use').get('xlink:href')
            if '#listing-alt' in icon:
                prop_ID = feature.find('span',class_='property-features__value').text.strip()
    except KeyError:
        prop_ID = None
    
    try:
        comment_div = soup.find('div', class_='listing-description__text')
        prop_desc = comment_div.text.strip()
    except:
        prop_desc = None
    
    current_datetime = datetime.now().strftime('%Y-%m-%d')

    return {
        "Listing ID": prop_ID, "Description": prop_desc, "Time_stamp": current_datetime}

def extractor_pics(soup, prop_id): # extracts from created urls
    try:
        script_tag = soup.find('script', string=re.compile(r'const serverVariables'))
        photo_data = []
        if script_tag:
            script_content = script_tag.string
            script_data2 = re.search(r'const serverVariables\s*=\s*({.*?});', script_content, re.DOTALL).group(1)
            json_data = json.loads(script_data2)
            photos = json_data['bundleParams']['galleryPhotos']
        
            # Extract all mediumUrl urls
            photo_urls = [item['mediumUrl'] for item in photos]
        
            # Store the extracted URLs with the listing ID            
            count = 0
            for url in photo_urls:
                count +=1
                photo_data.append({'Listing_ID': prop_id, 'Photo_Link': url})
                if count ==8:
                    break;
    except KeyError:
        print('Pictures not found')

    return photo_data

######################################Functions##########################################################
async def main():
    fieldnames = ['Listing ID', 'Description', 'Time_stamp']
    filename = "PrivComments4_2.csv"
    
    fieldnames_pics = ['Listing_ID', 'Photo_Link']
    filename_pics = "PrivPictures4_2.csv"
    
    ids = []
    semaphore = asyncio.Semaphore(500)

    async with aiohttp.ClientSession() as session:
        with open(filename, 'a', newline='', encoding='utf-8-sig') as csvfile, \
             open(filename_pics, 'a', newline='', encoding='utf-8-sig') as csvfile_pics:
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer_pics = csv.DictWriter(csvfile_pics, fieldnames=fieldnames_pics)
            
            writer.writeheader()
            writer_pics.writeheader()
            
            start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            async def process_province(prov):
                response_text = await fetch(session, prov, semaphore)
                home_page = BeautifulSoup(response_text, 'html.parser')
                new_links = []
                try:
                    inner = home_page
                    ul2 = inner.find('ul', class_='region-content-holder__unordered-list')
                    if ul2:
                        li_items2 = ul2.find_all('li', class_='region-content-holder__list')
                        for area2 in li_items2:
                            link2 = area2.find('a')
                            link2 = f"https://www.privateproperty.co.za{link2.get('href')}"
                            new_links.append(link2)
                    else:
                        new_links.append(prov)
                except aiohttp.ClientError as e:
                    print(f"Request failed for {prov}: {e}")

                async def process_link(x):
                    try:
                        x_response_text = await fetch(session, x, semaphore)
                        x_page = BeautifulSoup(x_response_text, 'html.parser')
                        num_pages = getPages(x_page, x)

                        for s in range(1, num_pages + 1):
                            if s % 10 == 0:
                                sleep_duration = random.randint(15, 25)
                                await asyncio.sleep(sleep_duration)

                            prop_page_text = await fetch(session, f"{x}?page={s}", semaphore)
                            x_prop = BeautifulSoup(prop_page_text, 'html.parser')
                            prop_contain = x_prop.find_all('a', class_='listing-result')
                            for prop in prop_contain:
                                data = getIds(prop)
                                ids.append(data)
                    except Exception as e:
                        print(f"An error occurred while processing link {x}: {e}")

                tasks = [process_link(x) for x in new_links]
                await asyncio.gather(*tasks)

            async def process_ids():
                count = 0

                async def process_id(list_id):
                    nonlocal count
                    count += 1
                    if count % 1000 == 0:
                        print(f"Processed {count} IDs, sleeping for 20 seconds...")
                        await asyncio.sleep(35)
                    list_url = f"https://www.privateproperty.co.za/for-sale/something/something/something/{list_id}"
                    try:
                        listing = await fetch(session, list_url, semaphore)
                        list_page = BeautifulSoup(listing, 'html.parser')
                        
                        # Extracting data and writing to the comments file
                        data = extractor(list_page, list_url)
                        writer.writerow(data)
                        
                        # Extracting photos and writing to the pictures file
                        photo_data = extractor_pics(list_page, list_id)
                        for photo in photo_data:
                            writer_pics.writerow(photo)
                        
                    except Exception as e:
                        print(f"An error occurred while processing ID {list_id}: {e}")

                tasks = [process_id(list_id) for list_id in ids]
                await asyncio.gather(*tasks)

            gp_links = ['https://www.privateproperty.co.za/for-sale/gauteng/pretoria/28',
                        'https://www.privateproperty.co.za/for-sale/gauteng/centurion/32',
                        'https://www.privateproperty.co.za/for-sale/gauteng/west-rand/839']
            await asyncio.gather(*(process_province(prov) for prov in gp_links))
            await process_ids()
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Start Time: {start_time}")
            print(f"End Time: {end_time}")

    connection_string = "DefaultEndpointsProtocol=https;AccountName=autotraderstorage;AccountKey=s7VIsKxE3aekvLIvwPJmo//Lbdv6Fd5Ip+tl/EmeFl0/RIFr90IQqw6iTAaPOdzMRTtwhK4CcHbj+AStMZIlNg==;BlobEndpoint=https://autotraderstorage.blob.core.windows.net/;QueueEndpoint=https://autotraderstorage.queue.core.windows.net/;TableEndpoint=https://autotraderstorage.table.core.windows.net/;FileEndpoint=https://autotraderstorage.file.core.windows.net/;"
    container_name = "comments-pics"
    
    # Uploading PrivComments.csv
    blob_name_comments = "PrivComments4_2.csv"
    blob_client_comments = BlobClient.from_connection_string(connection_string, container_name, blob_name_comments)
    with open(filename, "rb") as data:
        blob_client_comments.upload_blob(data, overwrite=True)
        print(f"File uploaded to Azure Blob Storage: {blob_name_comments}")

    # Uploading PrivPictures.csv
    blob_name_pics = "PrivPictures4_2.csv"
    blob_client_pics = BlobClient.from_connection_string(connection_string, container_name, blob_name_pics)
    with open(filename_pics, "rb") as data_pics:
        blob_client_pics.upload_blob(data_pics, overwrite=True)
        print(f"File uploaded to Azure Blob Storage: {blob_name_pics}")

# Running the main coroutine
asyncio.run(main())
