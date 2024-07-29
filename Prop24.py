from requests_html import AsyncHTMLSession
import re
from bs4 import BeautifulSoup
import json
import random
import csv
import math
from datetime import datetime
from azure.storage.blob import BlobClient
import asyncio
import time

######################################Functions##########################################################
def getPages(soup):
    try:
        num_listings = soup.find('div', class_='pull-right').find_all('span', class_='p24_bold')
        num_pgV = num_listings[-1].text.strip()
        pages = math.ceil(int(num_pgV) / 20)
        return pages
    except (ValueError, AttributeError) as e:
        print(f"Failed to parse number of pages - {e}")
        return 0

def getIDs_create_url(soup):
    thread_data = []
    p24_results = soup.find('div', class_='p24_results')
    if p24_results:
        col_9_div = p24_results.find('div', class_='col-9')
        if col_9_div:
            tile_containers = col_9_div.find_all('div', class_='p24_tileContainer')
            for tile in tile_containers:
                listing_number = tile.get('data-listing-number')
                if listing_number:
                    listing_number = listing_number.replace('P', '')
                    thread_data.append(listing_number)
    # Create URLs
    links = [f"https://www.property24.com/for-sale/langebaan-country-estate/langebaan/western-cape/10483/{i}?" for i in thread_data]
    return links

def extractor(soup): # extracts from created urls
    listing_id = soup.find('div', class_='p24_listing p24_regularListing').get('data-listingnumber')
    try:
        comment_24 = soup.find('div', class_='js_expandedText p24_expandedText hide')
        prop_desc = ' '.join(comment_24.stripped_strings)
    except:
        prop_desc = None

    current_datetime = datetime.now().strftime('%Y-%m-%d')

    return {
        "Listing ID": listing_id, "Description": prop_desc, "Time_stamp": current_datetime}

def extractor_pics(soup): # extracts from created urls
    listing_id = soup.find('div', class_='p24_listing p24_regularListing').get('data-listingnumber')
    photo_data = []
    try:
        photogrid_div = soup.find('div', class_='p24_mediaHolder hide').find('div', class_='p24_thumbnailContainer').find_all('div', class_='col-4 p24_galleryThumbnail')
        if photogrid_div:
            for x in photogrid_div:
                photo_url = x.find('img').get('lazy-src')
                photo_data.append({'Listing_ID': listing_id, 'Photo_Link': photo_url})
    except:
        print(f"No picture div found: {listing_id}")
    return photo_data

######################################Functions##########################################################
async def main():
    fieldnames = ['Listing ID', 'Description', 'Time_stamp']
    filename = "Prop24Comments2.csv"

    fieldnames_pics = ['Listing_ID', 'Photo_Link']
    filename_pics = "Prop24Pictures2.csv"
    extract_links = []
    
    session = AsyncHTMLSession()

    semaphore = asyncio.Semaphore(100)  # Adjust as necessary

    with open(filename, 'a', newline='', encoding='utf-8-sig') as csvfile, \
         open(filename_pics, 'a', newline='', encoding='utf-8-sig') as csvfile_pics:

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer_pics = csv.DictWriter(csvfile_pics, fieldnames=fieldnames_pics)

        writer.writeheader()
        writer_pics.writeheader()

        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        home_page = await session.get('https://www.property24.com/for-sale/advanced-search/results/?sp=pid%3d5%2c6%2c9%2c7%2c8%2c1%2c14%2c2%2c3')
        home_soup = BeautifulSoup(home_page.content, 'html.parser')
        pages = getPages(home_soup)


        async def fetch_page_links(pg):
            async with semaphore:
                link = f"https://www.property24.com/for-sale/advanced-search/results/p{pg}?sp=pid%3d5%2c6%2c9%2c7%2c8%2c1%2c14%2c2%2c3"
                home = await session.get(link)
                soup = BeautifulSoup(home.content, 'html.parser')
                extract_links.extend(getIDs_create_url(soup))
                if pg % 200 == 0:
                    print("Sleeping for 60 seconds after 200 pages links...")
                    await asyncio.sleep(60)

        await asyncio.gather(*(fetch_page_links(pg) for pg in range(1, pages + 1)))

        async def fetch_link_data(link):
            async with semaphore:
                try:
                    home_ex = await session.get(link)
                    soupex = BeautifulSoup(home_ex.content, 'html.parser')
                    comments = extractor(soupex)
                    photos = extractor_pics(soupex)
                    writer.writerow(comments)
                    for photo in photos:
                        writer_pics.writerow(photo)
                except Exception as e:
                    print(f"Error: {link}, {e}")

        tasks = []
        count = 0
        for l in extract_links:
            tasks.append(fetch_link_data(l))
            count += 1
            if count % 50 == 0:
                print("Sleeping extracted 50 links...")
                await asyncio.sleep(55)

        await asyncio.gather(*tasks)

        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"Start Time: {start_time}")
        print(f"End Time: {end_time}")

    connection_string = "DefaultEndpointsProtocol=https;AccountName=privateproperty;AccountKey=zX/k04pby4o1V9av1a5U2E3fehg+1bo61C6cprAiPVnql+porseL1NVw6SlBBCnVaQKgxwfHjZyV+AStKg0N3A==;BlobEndpoint=https://privateproperty.blob.core.windows.net/;QueueEndpoint=https://privateproperty.queue.core.windows.net/;TableEndpoint=https://privateproperty.table.core.windows.net/;FileEndpoint=https://privateproperty.file.core.windows.net/;"
    container_name = "comments-pics"

    # Uploading Prop24Comments.csv
    blob_name_comments = "Prop24Comments2.csv"
    blob_client_comments = BlobClient.from_connection_string(connection_string, container_name, blob_name_comments)
    with open(filename, "rb") as data:
        blob_client_comments.upload_blob(data, overwrite=True)
        print(f"File uploaded to Azure Blob Storage: {blob_name_comments}")

    # Uploading Prop24Pictures.csv
    blob_name_pics = "Prop24Pictures2.csv"
    blob_client_pics = BlobClient.from_connection_string(connection_string, container_name, blob_name_pics)
    with open(filename_pics, "rb") as data_pics:
        blob_client_pics.upload_blob(data_pics, overwrite=True)
        print(f"File uploaded to Azure Blob Storage: {blob_name_pics}")

if __name__ == "__main__":
    asyncio.run(main())
