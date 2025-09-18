import aiohttp
import asyncio
import re
from bs4 import BeautifulSoup
import json
import random
import csv
import gzip
import math
from datetime import datetime
from azure.storage.blob import BlobClient
import os

base_url = os.getenv("BASE_URL")
con_str_coms = os.getenv("CON_STR_COMS")

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
    try:
        # script_data = soup.find('script', type='application/ld+json').string
        # json_data = json.loads(script_data)
        # url = json_data['url']
        url = soup['href']

        prop_ID_match = re.search(r'/([^/]+)$', url)
        if prop_ID_match:
            return prop_ID_match.group(1)
    except Exception as e:
        print(f"Error extracting ID from {soup}: {e}")
    return None


def extractor(soup, url): # extracts from created urls
    # try:
    #     prop_ID = None
    #     prop_div = soup.find('div', class_='property-features')
    #     lists = prop_div.find('ul', class_='property-features__list')
    #     features = lists.find_all('li')
    #     for feature in features:
    #         icon = feature.find('svg').find('use').get('xlink:href')
    #         if '#listing-alt' in icon:
    #             prop_ID = feature.find('span',class_='property-features__value').text.strip()
    # except KeyError:
    #     prop_ID = None

    # try:
    #     comment_div = soup.find('div', class_='listing-description__text')
    #     prop_desc = comment_div.text.strip()
    # except:
    #     prop_desc = None

    # current_datetime = datetime.now().strftime('%Y-%m-%d')

    # return {
    #     "Listing ID": prop_ID, "Description": prop_desc, "Time_stamp": current_datetime}
    try:
        prop_ID = None
        prop_div = soup.find('div', class_='property-details')
        lists = prop_div.find('ul', class_='property-details__list')
        features = lists.find_all('li')
        for feature in features:
            icon = feature.find('svg').find('use').get('xlink:href')
            if '#listing-alt' in icon:
                prop_ID = feature.find('span', class_='property-details__value').text.strip()
    except KeyError:
        prop_ID = None
    
    prop_desc = None
    latitude = None
    longitude = None
    
    try:
        comment_div = soup.find('div', class_='listing-description__text')
        prop_desc = comment_div.text.strip()
        # script_tag = soup.find('div',class_='listing-details').find('script', type='application/ld+json')
        # photo_data = []
        # # print(script_tag.text)
        # if script_tag:
        #     script_content = script_tag.string
        #     # script_data2 = re.search(r'application/ld+json\s*=\s*({.*?});', script_content, re.DOTALL).group(1)
        #     json_data = json.loads(script_content)
        #     latitude = json_data['geo']['latitude']
        #     longitude = json_data['geo']['longitude']
    except:
        print('Error. Cannot find comments')
    
    current_datetime = datetime.now().strftime('%Y-%m-%d')
    
    return {"Listing ID": prop_ID, "Description": prop_desc, "Latitude": latitude, "Longitude": longitude,"Time_stamp": current_datetime}

    

def extractor_pics(soup, prop_id): # extracts from created urls
    try:
        prop_ID = None
        prop_div = soup.find('div', class_='property-details')
        lists = prop_div.find('ul', class_='property-details__list')
        features = lists.find_all('li')
        for feature in features:
            icon = feature.find('svg').find('use').get('xlink:href')
            if '#listing-alt' in icon:
                prop_ID = feature.find('span', class_='property-details__value').text.strip()
    except KeyError:
        prop_ID = None
    list_id = prop_ID
    
    try:
        photo_div = soup.find('div', class_='details-page-photogrid__photos')
        photo_data = []
        img_links = photo_div.find_all('img')
        count = 0
        for url in img_links:
            count += 1
            photo_data.append({'Listing_ID': list_id, 'Photo_Link': url.get('src')})
            if count == 8:
                break
        return photo_data        
    except KeyError:
        print('Pictures not found')
        return []


######################################Functions##########################################################
async def main():
    fieldnames = ['Listing ID', 'Description', 'Latitude', 'Longitude', 'Time_stamp']
    # filename = "PrivComments5.csv"
    gz_filename = "PrivComments5.csv.gz"

    fieldnames_pics = ['Listing_ID', 'Photo_Link']
    # filename_pics = "PrivPictures5.csv"
    gz_filename_pics = "PrivPictures5.csv.gz"

    ids = []
    semaphore = asyncio.Semaphore(500)

    async with aiohttp.ClientSession() as session:
        with gzip.open(gz_filename, 'wt', newline='', encoding='utf-8-sig') as gzfile, \
             gzip.open(gz_filename_pics, 'wt', newline='', encoding='utf-8-sig') as gzfile_pics:

            writer = csv.DictWriter(gzfile, fieldnames=fieldnames)
            writer_pics = csv.DictWriter(gzfile_pics, fieldnames=fieldnames_pics)

            writer.writeheader()
            writer_pics.writeheader()

            start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            provinces = {
                'kwazulu-natal': '2',
                'gauteng': '3',
                'western-cape': '4',
                'northern-cape': '5',
                'free-state': '6',
                'eastern-cape': '7',
                'Limpopo': '8',
                'north-west': '9',
                'mpumalanga': '10'
            }
            async def process_province(prov,p_num):
                # response_text = await fetch(session, f"{base_url}/commercial-sales/gauteng/{prov}", semaphore)
                # home_page = BeautifulSoup(response_text, 'html.parser')

                # links = []
                # ul = home_page.find('ul', class_='region-content-holder__unordered-list')
                # li_items = ul.find_all('li')
                # for area in li_items:
                #     link = area.find('a')
                #     link = f"{base_url}{link.get('href')}"
                #     links.append(link)

                new_links = []
                # for l in links:
                #     try:
                #         res_in_text = await fetch(session, f"{l}", semaphore)
                #         inner = BeautifulSoup(res_in_text, 'html.parser')
                #         ul2 = inner.find('ul', class_='region-content-holder__unordered-list')
                #         if ul2:
                #             li_items2 = ul2.find_all('li', class_='region-content-holder__list')
                #             for area2 in li_items2:
                #                 link2 = area2.find('a')
                #                 link2 = f"{base_url}{link2.get('href')}"
                #                 new_links.append(link2)
                #         else:
                new_links.append(f"{base_url}/for-sale/{prov}/{p_num}")
                    # except aiohttp.ClientError as e:
                    #     print(f"Request failed for {l}: {e}")

                async def process_link(x):
                    try:
                        x_response_text = await fetch(session, x, semaphore)
                        x_page = BeautifulSoup(x_response_text, 'html.parser')
                        num_pages = getPages(x_page, x)

                        for s in range(1, num_pages + 1):
                            if s % 10 == 0:
                                sleep_duration = random.randint(10, 15)
                                await asyncio.sleep(sleep_duration)

                            prop_page_text = await fetch(session, f"{x}?page={s}", semaphore)
                            x_prop = BeautifulSoup(prop_page_text, 'html.parser')
                            prop_contain = x_prop.find_all('a', class_='featured-listing')
                            prop_contain.extend(x_prop.find_all('a', class_='listing-result'))
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
                        await asyncio.sleep(55)
                    list_url = f"{base_url}/commercial-sales/something/something/something/{list_id}"
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

            await asyncio.gather(*(process_province(prov,p_num) for prov,p_num in provinces.items()))
            await process_ids()
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Start Time: {start_time}")
            print(f"End Time: {end_time}")

    connection_string = f"{con_str_coms}"
    container_name = "comments-pics"

    # Uploading PrivComments.csv
    blob_name_comments = "PrivComments5.csv.gz"
    blob_client_comments = BlobClient.from_connection_string(connection_string, container_name, blob_name_comments)
    with open(gz_filename, "rb") as data:
        blob_client_comments.upload_blob(data, overwrite=True)
        print(f"File uploaded to Azure Blob Storage: {blob_name_comments}")

    # Uploading PrivPictures.csv
    blob_name_pics = "PrivPictures5.csv.gz"
    blob_client_pics = BlobClient.from_connection_string(connection_string, container_name, blob_name_pics)
    with open(gz_filename_pics, "rb") as data_pics:
        blob_client_pics.upload_blob(data_pics, overwrite=True)
        print(f"File uploaded to Azure Blob Storage: {blob_name_pics}")

# Running the main coroutine
asyncio.run(main())
