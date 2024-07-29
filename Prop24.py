from requests_html import HTMLSession
from bs4 import BeautifulSoup
import math
import random
from datetime import datetime
import time
import csv
from azure.storage.blob import BlobClient

session = HTMLSession()

start_page = 1

def getPages(soup):
    try:
        num_listings = soup.find('div',class_='pull-right').find_all('span', class_='p24_bold')
        num_pgV = num_listings[-1].text.strip()
        pages = math.ceil(int(num_pgV) / 20)
        return pages
    except (ValueError, AttributeError) as e:
        print(f"Failed to parse number of pages for URL: {url} - {e}")
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
    #create Url
    links = []
    for i in thread_data:
        links.append(f"https://www.property24.com/for-sale/langebaan-country-estate/langebaan/western-cape/10483/{i}?")
    return links

def extractor(soup): # extracts from created urls
    listing_id = soup.find('div',class_='p24_listing p24_regularListing').get('data-listingnumber')
    try:
        comment_24 = soup.find('div', class_='js_expandedText p24_expandedText hide')
        prop_desc = ' '.join(comment_24.stripped_strings)
    except:
        prop_desc = None
    current_datetime = datetime.now().strftime('%Y-%m-%d')
    return {"Listing ID": listing_id, "Description": prop_desc, "Time_stamp": current_datetime}

def extractor_pics(soup): # extracts from created urls
    listing_id = soup.find('div',class_='p24_listing p24_regularListing').get('data-listingnumber')
    photo_data = []
    try:
        photogrid_div = soup.find('div',class_='p24_mediaHolder hide').find('div',class_='p24_thumbnailContainer').find_all('div',class_='col-4 p24_galleryThumbnail')
        if photogrid_div:
            for x in photogrid_div:
                photo_url = x.find('img').get('lazy-src')
                photo_data.append({'Listing_ID': listing_id, 'Photo_Link': photo_url})
    except:
        print(f"No picture div found: {listing_id}")
    return photo_data

def append_to_blob_and_reset(local_file, blob_client, fieldnames):
    with open(local_file, "rb") as data:
        blob_client.append_block(data.read())
    with open(local_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

home_page = session.get('https://www.property24.com/for-sale/advanced-search/results/?sp=pid%3d5%2c6%2c9%2c7%2c8%2c1%2c14%2c2%2c3')
home_soup = BeautifulSoup(home_page.content, 'html.parser')
pages = getPages(home_soup)

connection_string = "DefaultEndpointsProtocol=https;AccountName=privateproperty;AccountKey=zX/k04pby4o1V9av1a5U2E3fehg+1bo61C6cprAiPVnql+porseL1NVw6SlBBCnVaQKgxwfHjZyV+AStKg0N3A==;BlobEndpoint=https://privateproperty.blob.core.windows.net/;QueueEndpoint=https://privateproperty.queue.core.windows.net/;TableEndpoint=https://privateproperty.table.core.windows.net/;FileEndpoint=https://privateproperty.file.core.windows.net/;"
container_name = "comments-pics"
blob_name_comments = "Prop24Comments2.csv"
blob_name_pics = "Prop24Pictures2.csv"

blob_client_comments = BlobClient.from_connection_string(connection_string, container_name, blob_name_comments)
blob_client_pics = BlobClient.from_connection_string(connection_string, container_name, blob_name_pics)

fieldnames_comments = ['Listing ID', 'Description', 'Time_stamp']
filename_comments = "Prop24Comments2.csv"

fieldnames_pics = ['Listing_ID', 'Photo_Link']
filename_pics = "Prop24Pictures2.csv"

with open(filename_comments, 'w', newline='', encoding='utf-8-sig') as csvfile_comments, \
     open(filename_pics, 'w', newline='', encoding='utf-8-sig') as csvfile_pics:

    writer_comments = csv.DictWriter(csvfile_comments, fieldnames=fieldnames_comments)
    writer_pics = csv.DictWriter(csvfile_pics, fieldnames=fieldnames_pics)

    writer_comments.writeheader()
    writer_pics.writeheader()

    count = 0

    for pg in range(start_page, pages + 1):
        link = f"https://www.property24.com/for-sale/advanced-search/results/p{pg}?sp=pid%3d5%2c6%2c9%2c7%2c8%2c1%2c14%2c2%2c3"
        home = session.get(link)
        soup = BeautifulSoup(home.content, 'html.parser')
        extract_links = getIDs_create_url(soup)
        if pg % 200 == 0:
            print("Sleeping for 60 seconds after processing 200 pages...")
            time.sleep(60)

    print(f"links extracted: {len(extract_links)}")
    for l in extract_links:
        count += 1
        if count % 10 == 0:
            sleep_duration = random.randint(35, 50)
            time.sleep(sleep_duration)

        home_page = session.get(l)
        soupex = BeautifulSoup(home_page.content, 'html.parser')
        try:
            comments = extractor(soupex)
            photos = extractor_pics(soupex)

            writer_comments.writerow(comments)
            for photo in photos:
                writer_pics.writerow(photo)
            
            if count % 2000 == 0:
                append_to_blob_and_reset(filename_comments, blob_client_comments, fieldnames_comments)
                append_to_blob_and_reset(filename_pics, blob_client_pics, fieldnames_pics)

        except Exception as e:
            print(f"Error: {l}, {e}")


    # Append remaining data at the end
    append_to_blob_and_reset(filename_comments, blob_client_comments, fieldnames_comments)
    append_to_blob_and_reset(filename_pics, blob_client_pics, fieldnames_pics)
