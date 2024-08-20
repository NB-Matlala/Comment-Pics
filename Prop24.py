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

# Thread worker function
def worker(queue, results):
    while True:
        item = queue.get()
        if item is None:
            break
        url = item.get("url")
        extract_function = item.get("extract_function")
        try:
            response = session.get(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            result = extract_function(soup, url)
            if result:
                results.append(result)
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

def extractor(soup, url):
    # Initialize variables with default values
    prop_ID = erfSize = floor_size = rates = levy = None
    beds = baths = lounge = dining = garage = parking = storeys = None
    agent_name = agent_url = None

    try:
        prop_div = soup.find('div', class_='property-features')
        lists = prop_div.find('ul', class_='property-features__list')
        features = lists.find_all('li')
        for feature in features:
            icon = feature.find('svg').find('use').get('xlink:href')
            value = feature.find('span', class_='property-features__value').text.strip()
            if '#listing-alt' in icon:
                prop_ID = value
            elif '#property-type' in icon:
                prop_type = value
            elif '#erf-size' in icon:
                erfSize = value.replace('\xa0', ' ')
            elif '#property-size' in icon:
                floor_size = value.replace('\xa0', ' ')
            elif '#rates' in icon:
                rates = value.replace('\xa0', ' ')
            elif '#levies' in icon:
                levy = value.replace('\xa0', ' ')
    except Exception as e:
        print(f"Error extracting property features for {url}: {e}")

    try:
        prop_feat_div = soup.find('div', id='property-features-list')
        lists_feat = prop_feat_div.find('ul', class_='property-features__list')
        feats = lists_feat.find_all('li')
        for feat in feats:
            feat_icon = feat.find('svg').find('use').get('xlink:href')
            value = feat.find('span', class_='property-features__value').text.strip()
            if '#bedrooms' in feat_icon:
                beds = value
            elif '#bathroom' in feat_icon:
                baths = value
            elif '#lounges' in feat_icon:
                lounge = value
            elif '#dining' in feat_icon:
                dining = value
            elif '#garages' in feat_icon:
                garage = value
            elif '#covered-parking' in feat_icon:
                parking = value
            elif '#storeys' in feat_icon:
                storeys = value
    except Exception as e:
        print(f"Error extracting property features list for {url}: {e}")

    try:
        script_tag = soup.find('script', string=re.compile(r'const serverVariables'))
        if script_tag:
            script_content = script_tag.string
            script_data2 = re.search(r'const serverVariables\s*=\s*({.*?});', script_content, re.DOTALL).group(1)
            json_data = json.loads(script_data2)
            agent_name = json_data['bundleParams']['agencyInfo']['agencyName']
            agent_url = json_data['bundleParams']['agencyInfo']['agencyPageUrl']
            agent_url = f"https://www.privateproperty.co.za{agent_url}"
    except Exception as e:
        print(f"Error extracting agent information for {url}: {e}")

    return {
        "Listing ID": prop_ID, "Erf Size": erfSize, "Property Type": prop_type, "Floor Size": floor_size,
        "Rates and taxes": rates, "Levies": levy, "Bedrooms": beds, "Bathrooms": baths, "Lounges": lounge,
        "Dining": dining, "Garages": garage, "Covered Parking": parking, "Storeys": storeys, "Agent name": agent_name,
        "Agent Url": agent_url
    }

def getIds(soup):
    try:
        script_data = soup.find('script', type='application/ld+json').string
        json_data = json.loads(script_data)
        url = json_data['url']
        prop_ID_match = re.search(r'/([^/]+)$', url)
        if prop_ID_match:
            return prop_ID_match.group(1)
    except Exception as e:
        print(f"Error extracting ID from {soup}: {e}")
    return None

# Initialize thread queue and results list
queue = Queue()
results = []

response_text = session.get(f"https://www.privateproperty.co.za/for-sale/mpumalanga/4")
home_page = BeautifulSoup(response_text.content, 'html.parser')

links = []
ul = home_page.find('ul', class_='region-content-holder__unordered-list')
li_items = ul.find_all('li')
for area in li_items:
    link = area.find('a')
    link = f"https://www.privateproperty.co.za{link.get('href')}"
    links.append(link)

new_links = []
for l in links:
    try:
        res_in_text = session.get(f"{l}")
        inner = BeautifulSoup(res_in_text.content, 'html.parser')
        ul2 = inner.find('ul', class_='region-content-holder__unordered-list')
        if ul2:
            li_items2 = ul2.find_all('li', class_='region-content-holder__list')
            for area2 in li_items2:
                link2 = area2.find('a')
                link2 = f"https://www.privateproperty.co.za{link2.get('href')}"
                new_links.append(link2)
        else:
            new_links.append(l)
    except Exception as e:
        print(f"Request failed for {l}: {e}")

for x in new_links:
    try:
        land = session.get(x)
        land_html = BeautifulSoup(land.content, 'html.parser')
        pgs = getPages(land_html, x)
        for p in range(1, pgs + 1):
            home_page = session.get(f"{x}?page={p}")
            soup = BeautifulSoup(home_page.content, 'html.parser')
            prop_contain = soup.find_all('a', class_='listing-result')
            for x_page in prop_contain:
                prop_id = getIds(x_page)
                if prop_id:
                    list_url = f"https://www.privateproperty.co.za/for-sale/something/something/something/{prop_id}"
                    queue.put({"url": list_url, "extract_function": extractor})
    except Exception as e:
        print(f"Failed to process URL {x}: {e}")

# Start threads
num_threads = 10  # Adjust the number of threads based on your system's capabilities
threads = []
for i in range(num_threads):
    t = threading.Thread(target=worker, args=(queue, results))
    t.start()
    threads.append(t)

# Block until all tasks are done
queue.join()

# Stop workers
for i in range(num_threads):
    queue.put(None)
for t in threads:
    t.join()

# Write results to CSV
csv_filename = 'prop2.csv'
with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = results[0].keys() if results else []
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for result in results:
        writer.writerow(result)

# Upload to Azure Blob Storage
blob_connection_string = "DefaultEndpointsProtocol=https;AccountName=privateproperty;AccountKey=zX/k04pby4o1V9av1a5U2E3fehg+1bo61C6cprAiPVnql+porseL1NVw6SlBBCnVaQKgxwfHjZyV+AStKg0N3A==;BlobEndpoint=https://privateproperty.blob.core.windows.net/;QueueEndpoint=https://privateproperty.queue.core.windows.net/;TableEndpoint=https://privateproperty.table.core.windows.net/;FileEndpoint=https://privateproperty.file.core.windows.net/;"
blob = BlobClient.from_connection_string(
    blob_connection_string,
    container_name="privateprop",
    blob_name=csv_filename
)
with open(csv_filename, "rb") as data:
    blob.upload_blob(data, overwrite=True)

print("CSV file uploaded to Azure Blob Storage.")
