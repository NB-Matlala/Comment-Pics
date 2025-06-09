from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import json
from datetime import datetime, timedelta
import re
import pandas as pd
from selenium_stealth import stealth
from azure.storage.blob import BlobClient
import os
from selenium.common.exceptions import TimeoutException, NoSuchElementException


# current_datetime1 = datetime.now()
# Add 2 hours to the current datetime
new_datetime = datetime.now() + timedelta(hours=2)
current_datetime = new_datetime.strftime('%Y-%m-%d')

failed_pgs = []

data_list = []
total_listings_f=0

# Function to set up WebDriver
def setup_driver():
    chrome_options = Options()
    ###
    chrome_options.add_argument("--disable-gpu")
    #chrome_options.add_argument("--disable-webgl")
    #chrome_options.add_argument("--use-gl=swiftshader")
    chrome_options.add_argument("--enable-unsafe-webgpu")
    chrome_options.add_argument("--headless")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument("--log-level=3")  # Suppress logs
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # Hides Selenium automation
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])  
    chrome_options.add_experimental_option("useAutomationExtension", False)
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)


#part 1--get total pages--
def get_total_pages():
    num_pages=800
    driver = setup_driver()
    stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )     
    try:
        # Go to the URL
        url = "https://www.cars.co.za/usedcars/commercial/"
        driver.get(url)

        # Give the page some time to load
        time.sleep(3)    
        page_html = driver.page_source
        
        soup = BeautifulSoup(page_html, "html.parser")

        # Find the element containing the total number of listings
        search_info_div = soup.find('div', class_='py-3 fixed-width-container u-border-bottom-light MainSearchContent_searchInfo__KWg0l')
        
        
        if search_info_div:
            # Extract the text containing the total number of listings
            search_info_text = search_info_div.find('p', class_='MainSearchContent_total__HLG4o').text
            # Use regex to extract the total number of listings
            total_listings = re.search(r'Displaying \d+ - \d+ of (\d+) results', search_info_text)
            if total_listings:
                total_listings = int(total_listings.group(1))
                total_listings_f=total_listings+5
                print(total_listings)
                
                if total_listings is not None:
                    # Calculate the number of pages based on the assumption that each page displays 20 listings
                    num_pages = (total_listings + 19) // 20  # Round up using integer division
                else:    
                    num_pages=800
                    
        
            else:
                print("Total listings not found.")
                num_pages=800
                total_listings_f=0

        else:
            print("Search information not found.")
            num_pages=800
        

    finally:
        # Close the browser
        driver.quit()
        
    return total_listings_f,num_pages    

total_listings_f,num_pages =get_total_pages()


###part 1 done#####
    
import pandas as pd
import urllib
from datetime import datetime



##part 2 extraction## closing browser
import threading
def extract_urls(soup,data_list):
    """Extract all URLs containing '/for-sale/' from the page HTML"""
    #soup = BeautifulSoup(content, "html.parser")
    try:
        data_dict = json.loads(soup.find('script', {'id': '__NEXT_DATA__'}).string)
        datap = data_dict['props']['initialState']['searchCarReducer']['searchResults']
        data = datap.get('data', [])
        for item in data:
            attributes = item.get('attributes', {})       

            Car_ID = item.get('id')
            title = attributes.get('title')
            region = attributes.get('agent_locality')      
            brand = attributes.get('make')
            model = attributes.get('model')
            variant =attributes.get('variant_short')
            price = attributes.get('price')
            dealer=attributes.get('agent_name')
            car_type = attributes.get('new_or_used')
            registration_year = re.search(r'\d+', title)
            registration_year = registration_year.group()
            mileage = attributes.get('mileage')
            transmission = attributes.get('transmission')
            fuel_type = attributes.get('fuel_type')
            seller_type=attributes.get('seller_type')
            manufacturers_colour =None
            manufacturers_colour1 = attributes.get('colour')
            if manufacturers_colour!=None:
                manufacturers_colour = attributes.get('colour')        
            body_type = attributes.get('body_type')
            latitude=attributes.get('agent_coords_0_coord')
            longitude=attributes.get('agent_coords_1_coord')
            # Add 2 hours to the current datetime
            new_datetime = datetime.now() + timedelta(hours=2)
            current_datetime = new_datetime.strftime('%Y-%m-%d')
            province = attributes.get('province')
            dealer_attri = item.get('relationships', {}) 
            dealer_id= dealer_attri['seller']['data']['id']     
            if Car_ID is not None:  
                data_list.append({
                        'Car_ID': Car_ID,
                        'Title': title,
                        'Region': region,
                        'Make': brand,
                        'Model': model,
                        'Variant': variant,
                        'Suburb': None,
                        'Province': province,
                        'Price': price,
                        'ExpectedPaymentPerMonth': None,
                        'CarType': car_type,
                        'RegistrationYear': registration_year,
                        'Mileage': mileage,
                        'Transmission': transmission,
                        'FuelType': fuel_type,
                        'PriceRating': None,
                        'Dealer': dealer,
                        'LastUpdated': None,
                        'PreviousOwners': None,
                        'ManufacturersColour': manufacturers_colour,
                        'BodyType': body_type,
                        'ServiceHistory': None,
                        'WarrantyRemaining': None,
                        'IntroductionDate': latitude,
                        'EndDate': longitude,
                        'ServiceIntervalDistance': None,
                        'EnginePosition': None,
                        'EngineDetail': seller_type,
                        'EngineCapacity': None,
                        'CylinderLayoutAndQuantity': None,
                        'FuelTypeEngine': fuel_type,
                        'FuelCapacity': None,
                        'FuelConsumption': None,
                        'FuelRange': None,
                        'PowerMaximum': None,
                        'TorqueMaximum': None,
                        'Acceleration': None,
                        'MaximumSpeed': None,
                        'CO2Emissions': None,
                        'Version': 1,
                        'DealerUrl':dealer_id,
                        'Timestamp': current_datetime
                    })
    except Exception as e:
        print(f"Error processing page {e}") 
        return False
       
    urls = [a['href'] for a in soup.find_all('a', href=True) if '/for-sale/' in a['href']]
    return urls, data_list

st_time = datetime.now().strftime('%H:%M:%S')


# Threads
def scrape_page(page_num, data_list):
    driver = setup_driver()
    stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )

    url = f"https://www.cars.co.za/usedcars/?sort=date_d&price_type=listing_price&commercial_type=commercial&P={page_num}"  


    max_retries = 3
    wait_time = 15

    for attempt in range(max_retries):
        try:    
            driver.implicitly_wait(wait_time)

            driver.get(url)
            time.sleep(2)

            print('Page accessed.')
            content=driver.page_source
            soup = BeautifulSoup(content, "html.parser")
            
            if soup is not None:
                extract_urls(soup, data_list)
            else:
                driver.refresh()
                content=driver.page_source
                soup = BeautifulSoup(content, "html.parser") 
                extract_urls(soup, data_list) 

            if extract_urls(soup, data_list) == False:
                failed_pgs.append(url)

            driver.quit()
            break
        except NoSuchElementException:
            print(f"Timeout on attempt {attempt + 1}, retrying...")
            driver.quit()
            driver = setup_driver()
            stealth(driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
            )
            time.sleep(2)  # short pause before retry

def failed_scrape(link, data_list):
    max_retries = 3
    wait_time = 15
   
    driver = setup_driver()
    stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,)

    for attempt in range(max_retries):
        try:
            driver.implicitly_wait(wait_time)
            driver.get(link)
            time.sleep(2)

            print(f'{link}:\nAccessed.\n')
            content=driver.page_source
            soup = BeautifulSoup(content, "html.parser")
            
            if soup is not None:
                extract_urls(soup, data_list)
            else:
                driver.refresh()
                content=driver.page_source
                soup = BeautifulSoup(content, "html.parser") 
                extract_urls(soup, data_list) 

            if extract_urls(soup, data_list) == False:
                failed_pgs.append(link)

            driver.quit()
            break
        except NoSuchElementException:
            print(f"Timeout on attempt {attempt + 1}, retrying...")
            driver.quit()
            driver = setup_driver()
            stealth(driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
            )
            time.sleep(2)  # short pause before retry
    



# List to store extracted data
car_data_list = []
threads = []
num_threads = 7  # Number of threads to run in parallel


connection_string = os.getenv("CON_STR_COMS")
container_name = "comments-pics"

# Create threads
print(f"running main details extract")

import csv
import os
import subprocess
sleep_count= 0
for n in range(1, num_pages + 1):    
    thread = threading.Thread(target=scrape_page, args=(n, car_data_list))
    threads.append(thread)
    thread.start()

    # Limit active threads
    if len(threads) >= num_threads:
        for t in threads:
            t.join()
        threads = []  # Reset thread list after joining

    # Sleep
    
    if n % 300 == 0:
        print(f"Processed {n} pages, sleeping for 35 seconds...")
        sleep_count+=1
        filename_ext = f'ExtractedCars_{sleep_count}_{current_datetime}.csv'
        # ingest data


        df = pd.DataFrame(car_data_list)
        df.to_csv(f"{filename_ext}", encoding='utf-8', index=False)

        # car_data_list.to_csv(f"{filename_ext}", encoding='utf-8', index=False)
        
        blob_name = f"{filename_ext}"
        blob_client_ = BlobClient.from_connection_string(connection_string, container_name, blob_name)
        with open(filename_ext, "rb") as data:
            blob_client_.upload_blob(data, overwrite=True)
            print(f"File uploaded to Azure Blob Storage: {filename_ext}")
        
        
        car_data_list=[]


        time.sleep(35)

    elif n>=num_pages and len(failed_pgs)==0:
        filename_ext = f'ExtractedCars_F_{current_datetime}.csv'
        # ingest data
        df = pd.DataFrame(car_data_list)
        df.to_csv(f"{filename_ext}", encoding='utf-8', index=False)
        
        blob_name = f"{filename_ext}"
        blob_client_ = BlobClient.from_connection_string(connection_string, container_name, blob_name)
        with open(filename_ext, "rb") as data:
            blob_client_.upload_blob(data, overwrite=True)
            print(f"File uploaded to Azure Blob Storage: {filename_ext}")
        

        car_data_list =[]      

    elif n>=num_pages and len(failed_pgs)>0:
        print(f"Failed urls: {len(failed_pgs)}\n{failed_pgs}")
        for n in range(0, len(failed_pgs)):    
            thread = threading.Thread(target=failed_scrape, args=(failed_pgs[n], car_data_list))
            threads.append(thread)
            thread.start()

            # Limit active threads
            if len(threads) >= num_threads:
                for t in threads:
                    t.join()
                threads = []  # Reset thread list after joining

        filename_ext = f'ExtractedCars_Fail{len(failed_pgs)}_{current_datetime}.csv'
        # ingest data
        df = pd.DataFrame(car_data_list)
        df.to_csv(f"{filename_ext}", encoding='utf-8', index=False)

        # car_data_list.to_csv(f"{filename_ext}", encoding='utf-8', index=False)
        
        blob_name = f"{filename_ext}"
        blob_client_ = BlobClient.from_connection_string(connection_string, container_name, blob_name)
        with open(filename_ext, "rb") as data:
            blob_client_.upload_blob(data, overwrite=True)
            print(f"File uploaded to Azure Blob Storage: {filename_ext}")


        car_data_list=[]
        
    # elif n % 700 == 0:
    #     print(f"Processed {n} pages, sleeping for 60 seconds...")
    #     time.sleep(250)        

# Wait for remaining threads to finish
for t in threads:
    t.join()

 

print(f"Start Time: {st_time}") 
print(f"End Time: {datetime.now().strftime('%H:%M:%S')}")
