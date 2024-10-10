from bs4 import BeautifulSoup
from requests_html import HTMLSession
import re
import requests
import math
import json
import time
import threading
from queue import Queue
from datetime import datetime
import csv
from azure.storage.blob import BlobClient

session = HTMLSession()


# def extractor_pics(soup): # extracts from created urls
#     listing_id = soup.find('div',class_='p24_listing p24_regularListing').get('data-listingnumber')
#     photo_data = []
#     try:
#         photogrid_div = soup.find('div',class_='p24_mediaHolder hide').find('div',class_='p24_thumbnailContainer').find_all('div',class_='col-4 p24_galleryThumbnail')
#         if photogrid_div:
#             counter = 0
#             for x in photogrid_div:
#                 photo_url = x.find('img').get('lazy-src')
#                 photo_data.append({'Listing_ID': listing_id, 'Photo_Link': photo_url})
#                 counter += 1
#                 if counter == 3: #8
#                     break
#     except:
#         print(f"No picture div found: {listing_id}")
#     return photo_data

# def extractor(soup):
#     listing_id = soup.find('div',class_='p24_listing p24_regularListing').get('data-listingnumber')
#     try:
#         comment_24 = soup.find('div', class_='js_expandedText p24_expandedText hide')
#         prop_desc = ' '.join(comment_24.stripped_strings)
#     except:
#         prop_desc = None
#     current_datetime = datetime.now().strftime('%Y-%m-%d')
#     data_desc= {"Listing ID": listing_id,
#                 "Description": prop_desc,
#                 "Time_stamp": current_datetime}
#     return data_desc


# ######################################## Functions ###############################################################

# links = ['https://www.property24.com/for-sale/quigney/east-london/eastern-cape/6577/114696838',
#          'https://www.property24.com/for-sale/quigney/east-london/eastern-cape/6577/113747758',
#          'https://www.property24.com/for-sale/quigney/east-london/eastern-cape/6577/113470214']

# for l in links:
#     response = session.get(l)
#     soup = BeautifulSoup(response.content, 'html.parser')
#     print(f"Pics: {extractor_pics(soup)}\nDes: {extractor(soup)}")

for a in range(1,4):
    print('Hello World at', datetime.now().strftime('%H:%M'))

    # token = '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'
    
    # headers = {
    #     'Authorization': f'Bearer {token}',
    #     'Accept': 'application/vnd.github.v3+json'
    # }
    
    # response = requests.post(
    #     'https://api.github.com/repos/NB-Matlala/Comment-Pics/dispatches',
    #     headers=headers
    # )
    
