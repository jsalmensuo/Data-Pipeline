import requests
from bs4 import BeautifulSoup
from utils.file_utils import save_to_json
import time
import random

# Function to extract outage data from a single page
def extract_outage_data(page_number):
    # Construct the URL of the page
    url = f"https://savonvoima.fi/kategoria/hairiot/page/{page_number}/"
    
    # Send a GET request to the page
    response = requests.get(url)
    
    # If the request is successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find all div blocks with the class 'uutisnosto-sisalto sisennys'
        divs = soup.find_all('div', class_='uutisnosto-sisalto sisennys')
        
        outage_data = []
        
        # Loop through each div and extract the first <p> tag
        for div in divs:
            # Find the first <p> tag inside the div
            p_tag = div.find('p')
            
            if p_tag:
                # Extract the date, time, and additional information
                date_time_info = p_tag.text.strip()
                outage_data.append(date_time_info)
                        # Print the data to check if it's being extracted
                print(f"Page {page_number} data: {outage_data}")
        
        return outage_data
    else:
        print(f"Failed to retrieve page {page_number}")
        return []

# Scrape pages from 2 to 89
def scrape_outage_data():
    all_outages = []
    for page_number in range(2, 90):
        print(f"Scraping page {page_number}...")
        outages_on_page = extract_outage_data(page_number)
        all_outages.extend(outages_on_page)
        time.sleep(random.uniform(1, 3))  # Delay between 1 and 3 seconds    


    return all_outages