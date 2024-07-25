import requests
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

async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


######################################Functions##########################################################

def getPages(soupPage, url):
    try:
        num_pg = soupPage.find('div', class_='development-results-page-grid__result-container-header').find('div', class_='development-results-page-grid__result-container-header-count')
        num_pgV = num_pg.text.strip()
        num_pgV = num_pgV[num_pgV.index('of '):]
        num_pgV = num_pgV.replace('of ', '').replace(' results', '')
        pages = math.ceil(int(num_pgV) / 20)
        return pages
    except (ValueError, AttributeError) as e:
        print(f"Failed to parse number of pages for URL: {url} - {e}")
        return 0

def out_extractor(soup):
    title = soup.get('title')
    title = title.replace(' for sale', '')
    list_url = soup.get('href')
    list_url = f"https://www.privateproperty.co.za{list_url}"
    list_id_match = re.search(r'/([^/]+)\.htm$', list_url)
    list_id = list_id_match.group(1)
    detail_div = soup.find('div', class_='development-result-card-link__info-grid--info')
    locality = detail_div.find('div', class_='txt-base-regular development-result-card-link__info-grid--info-shape').text.strip()
    price_div = soup.find('h3', class_='development-result-card-link__info-grid--info-price')

    try:
        price_from = price_div.find('span', class_='development-result-card-link__info-grid--info-price-from truncate').text.strip()
        price_from = price_from.replace('\xa0', '')
    except:
        price_from = None

    try:
        price_to = price_div.find('span', class_='development-result-card-link__info-grid--info-price-to truncate').text.strip()
        price_to = price_to.replace('\xa0', '')
        price_to = price_to.replace('- ', '')
    except:
        price_to = None

    property_type = 'Developments'

    current_datetime = datetime.now().strftime('%Y-%m-%d')

    return {
        "Listing ID": list_id, "Title": title, "Property Type": property_type, "Price From": price_from, "Price To": price_to,
        "Suburb": locality, "URL": list_url, "Time_stamp":current_datetime}


######################################Functions##########################################################

async def main():
    fieldnames = ['Listing ID', 'Title', 'Property Type', 'Price From', 'Price To', 'Suburb', 'URL', 'Time_stamp']
    filename = "PrivatePropDev.csv"

    async with aiohttp.ClientSession() as session:
        with open(filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            async def process_province(prov):
                response_text = await fetch(session, f"https://www.privateproperty.co.za/developments/Gauteng/{prov}.htm")
                new_links = []
                new_links.append(f"https://www.privateproperty.co.za/developments/Gauteng/{prov}.htm")

                async def process_link10(x):
                    try:
                        x = f"{x}?"
                        x_response_text = await fetch(session, x)
                        x_page = BeautifulSoup(x_response_text, 'html.parser')
                        num_pages = getPages(x_page, x)

                        for s in range(1, num_pages + 1):
                            if s % 10 == 0:
                                sleep_duration = random.randint(10, 15)
                                await asyncio.sleep(sleep_duration)

                            prop_page_text = await fetch(session, f"{x}page={s}")
                            x_prop = BeautifulSoup(prop_page_text, 'html.parser')
                            prop_contain = x_prop.find_all('a', class_='development-result-card-link')
                            for prop in prop_contain:
                                data = out_extractor(prop)
                                writer.writerow(data)
                    except Exception as e:
                        print(f"An error occurred while processing link {x}: {e}")

                tasks = []
                for x in new_links:
                    tasks.extend([process_link10(x)])

                await asyncio.gather(*tasks)

            await asyncio.gather(*(process_province(prov) for prov in range(2, 11)))
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Start Time: {start_time}")
            print(f"End Time: {end_time}")


        # Upload the CSV file to Azure Blob Storage
        connection_string = "DefaultEndpointsProtocol=https;AccountName=privateproperty;AccountKey=zX/k04pby4o1V9av1a5U2E3fehg+1bo61C6cprAiPVnql+porseL1NVw6SlBBCnVaQKgxwfHjZyV+AStKg0N3A==;BlobEndpoint=https://privateproperty.blob.core.windows.net/;QueueEndpoint=https://privateproperty.queue.core.windows.net/;TableEndpoint=https://privateproperty.table.core.windows.net/;FileEndpoint=https://privateproperty.file.core.windows.net/;"
        container_name = "privatepropdev"
        blob_name = "PrivatePropDev.csv"

        blob_client = BlobClient.from_connection_string(connection_string, container_name, blob_name)

        with open(filename, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            print(f"File uploaded to Azure Blob Storage: {blob_name}")



        # body = ''
        # requests.post('https://prod2-09.southafricanorth.logic.azure.com:443/workflows/623f55b4346742178048b209a003f895/triggers/When_a_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=PxhThWSLOC3sS3JAg54Z3uZEhTvU9zAbNIhkaAhMPN0', json=body)



# Running the main coroutine
asyncio.run(main())


##############################################################Outside#############################################################################

async def fetch2(session, url, semaphore):
    async with semaphore:
        async with session.get(url) as response:
            return await response.text()

######################################Functions##########################################################
def getPages2(soupPage, url):
    try:
        num_pg = soupPage.find('div', class_='development-results-page-grid__result-container-header').find('div', class_='development-results-page-grid__result-container-header-count')
        num_pgV = num_pg.text.strip()
        num_pgV = num_pgV[num_pgV.index('of '):]
        num_pgV = num_pgV.replace('of ', '').replace(' results', '')
        pages = math.ceil(int(num_pgV) / 20)        
        return pages
    except (ValueError, AttributeError) as e:
        print(f"Failed to parse number of pages for URL: {url} - {e}")
        return 0

def getIds(soup):
    list_url = soup.get('href')
    list_id_match = re.search(r'/([^/]+)\.htm$', list_url)
    list_id = list_id_match.group(1)

    return list_id

def extractor(soup, list_url ,listID): # extracts from created urls
    loc_div = soup.find('nav', class_='details-page-breadcrumb-nav')
    crumbs = loc_div.find('ul', class_='breadcrumbs-list')

    locs = crumbs.find_all('li', class_='breadcrumb breadcrumb--shape')
    for li in locs:

        region = li.find('a', class_='txt-small-regular breadcrumb__shape-link breadcrumb--last')
        if region:
               regV = region.text.strip()
        province = li.find('a', class_='txt-small-regular breadcrumb__shape-link')
        if province:
            proV = province.text.strip()

    unit_div = soup.find('div', class_='listing-details').find('div', class_='listing-details__left-col')
    num_units = unit_div.find('p').findNext('p')
    units = num_units.text.strip()

    script_tag = soup.find('script', string=re.compile(r'const serverVariables'))
    list_id = listID
    if script_tag:
        script_content = script_tag.string
        script_data2 = re.search(r'const serverVariables\s*=\s*({.*?});', script_content, re.DOTALL).group(1)
        json_data = json.loads(script_data2)
        try:
            agent_name = json_data['bundleParams']['agencyInfo']['agencyName']
            agent_url = json_data['bundleParams']['agencyInfo']['agencyPageUrl']
            agent_url = f"https://www.privateproperty.co.za{agent_url}"
        except:
            agent_name = None
            agent_url = None

        current_datetime = datetime.now().strftime('%Y-%m-%d')

    return {
        "Listing ID": list_id, "Region": regV, "Province": proV, "Units Available": units, "Agent Name": agent_name,
        "Agent Url": agent_url, "URL": list_url, "Time_stamp": current_datetime}

######################################Functions##########################################################
async def main2():
    fieldnames2 = ['Listing ID', 'Region', 'Province', 'Units Available', 'Agent Name', 'Agent Url', 'URL', 'Time_stamp']
    filename2 = "PrivatePropRes(Inside).csv"
    ids = []
    semaphore2 = asyncio.Semaphore(500)

    async with aiohttp.ClientSession() as session2:
        with open(filename2, 'a', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames2)
            writer.writeheader()
            start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            async def process_province2(prov):
                response_text = await fetch(session2, f"https://www.privateproperty.co.za/developments/Gauteng/{prov}.htm")
                new_links = []
                new_links.append(f"https://www.privateproperty.co.za/developments/Gauteng/{prov}.htm")

                async def process_link(x):
                    try:
                        x_response_text = await fetch2(session2, x, semaphore2)
                        x_page = BeautifulSoup(x_response_text, 'html.parser')
                        num_pages = getPages2(x_page, x)

                        for s in range(1, num_pages + 1):
                            if s % 10 == 0:
                                sleep_duration = random.randint(10, 15)
                                await asyncio.sleep(sleep_duration)

                            prop_page_text = await fetch2(session2, f"{x}?page={s}", semaphore2)
                            x_prop = BeautifulSoup(prop_page_text, 'html.parser')
                            prop_contain = x_prop.find_all('a', class_='development-result-card-link')
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
                    list_url = f"https://www.privateproperty.co.za/developments/Garsfontein/the-oval-luxury-apartments/{list_id}.htm"
                    try:
                        listing = await fetch2(session2, list_url, semaphore2)
                        list_page = BeautifulSoup(listing, 'html.parser')
                        data = extractor(list_page, list_url, list_id)
                        writer.writerow(data)
                    except Exception as e:
                        print(f"An error occurred while processing ID {list_id}: {e}")

                tasks = [process_id(list_id) for list_id in ids]
                await asyncio.gather(*tasks)

            await asyncio.gather(*(process_province2(prov) for prov in range(2, 11)))
            await process_ids()
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"Start Time: {start_time}")
            print(f"End Time: {end_time}")

    connection_string = "DefaultEndpointsProtocol=https;AccountName=privateproperty;AccountKey=zX/k04pby4o1V9av1a5U2E3fehg+1bo61C6cprAiPVnql+porseL1NVw6SlBBCnVaQKgxwfHjZyV+AStKg0N3A==;BlobEndpoint=https://privateproperty.blob.core.windows.net/;QueueEndpoint=https://privateproperty.queue.core.windows.net/;TableEndpoint=https://privateproperty.table.core.windows.net/;FileEndpoint=https://privateproperty.file.core.windows.net/;"
    container_name = "privatepropdev"
    blob_name = "PrivatePropRes(Inside).csv"

    blob_client = BlobClient.from_connection_string(connection_string, container_name, blob_name)

    with open(filename2, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
        print(f"Inside File uploaded to Azure Blob Storage: {blob_name}")



# Running the main coroutine
asyncio.run(main2())
