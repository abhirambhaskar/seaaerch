import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.selector import Selector
from goose3 import Goose
import spacy
import logging
import csv
import tldextract
from PIL import Image
from io import BytesIO
import requests
from bs4 import BeautifulSoup
import json
import uuid
import os
import httpx
import datetime
from ftplib import FTP
from urllib.parse import urljoin
from scrapy_selenium import SeleniumRequest, SeleniumMiddleware
from middlewares import SeleniumMiddleware
from requests.exceptions import RequestException
from PIL import Image
from nudenet import NudeClassifier
import uuid
from io import BytesIO
from concurrent.futures import wait,ThreadPoolExecutor
class SeleniumMiddleware(SeleniumMiddleware):
    def __init__(self):
        super().__init__()

#logging.getLogger('selenium.webdriver.remote.remote_connection').setLevel(logging.ERROR)


# Initialize the classifier
classifier = NudeClassifier()

# Make sure the temp image directory exists
os.makedirs('imgtemp', exist_ok=True)

def is_nsfw(image_url):
    """Check if an image is NSFW."""
    try:
        # Download the image
        response = requests.get(image_url)
        image = Image.open(BytesIO(response.content))
        
        # Convert RGBA or P images to RGB
        if image.mode in ('RGBA', 'P'):
            image = image.convert('RGB')

        # Generate a random image file name
        temp_filename = f"imgtemp/{str(uuid.uuid4())}.jpg"
        
        # Save the image
        image.save(temp_filename)
        
        # Run the classifier
        result = classifier.classify(temp_filename)
        os.remove(temp_filename)  # Remove the temporary file
        nsfw_score = result.get(temp_filename, {}).get('unsafe', 0)

        # You can adjust this threshold depending on how strict you want to be
        return nsfw_score > 0.5
    except Exception as e:
        print(f"Error processing image {image_url}: {e}")
        return None


class MySpider(CrawlSpider):
    name = 'spider'

    # Define the user-agent
    user_agent = 'AksharaBot/1.0'

    custom_settings = {
    'USER_AGENT': 'AksharaBot/1.0',
    'DOWNLOAD_TIMEOUT': 60,
    'CONCURRENT_REQUESTS': 40,
    'DOWNLOAD_DELAY': 0.2,
    'CONCURRENT_REQUESTS_PER_DOMAIN': 10,
    'CONCURRENT_REQUESTS_PER_IP': 10,
    'AUTOTHROTTLE_ENABLED': True,
    'AUTOTHROTTLE_START_DELAY': 1.0,
    'AUTOTHROTTLE_MAX_DELAY': 60.0,
    'AUTOTHROTTLE_TARGET_CONCURRENCY': 0.5,
    'AUTOTHROTTLE_DEBUG': False,
    'DOWNLOADER_MIDDLEWARES': {
        'middlewares.SeleniumMiddleware': 100
    },
    'MAX_LINKS_PER_DOMAIN' : 30,
    'SELENIUM_DRIVER_NAME': 'chrome',
    'SELENIUM_DRIVER_EXECUTABLE_PATH': r'C:\Program Files\Google\Chrome\Application\chromedriver.exe',  # replace with your chromedriver path
    'SELENIUM_DRIVER_ARGUMENTS': ['--headless']
    }




    rules = (
        Rule(LinkExtractor(deny=['/watch?v=']), callback='parse_item', follow=True),
        Rule(LinkExtractor(allow_domains=['www.google.com']), callback='parse_item', follow=True),
        Rule(LinkExtractor(), callback='parse_item', follow=True),
    )

    start_urls = []
   

    def __init__(self, *args, **kwargs):
        super(MySpider, self).__init__(*args, **kwargs)
        self.data_list = []
        self.image_data_list = []
        self.crawled_count = 0
        self.crawledimg_count = 0
        self.nlp = spacy.load("en_core_web_sm")  # Load English model
        self.processed_urls = set()  # Set to store processed URLs
        self.read_start_urls()
        self.country_code = self.get_country_code()
        self.fieldnames = ['id', 'url', 'title', 'description', 'content', 'keyword', 'seo_rank', 'image_url', 'country']

        self.fieldnames_image = ['id', 'name', 'imageurl', 'link', 'keywords','safe_search','country']

        # Initialize temporary CSV file to store crawled images
        now = datetime.datetime.now()
       

        # Format as a string
        timestamp = now.strftime("%Y%m%d%H%M%S")
        timestampimg = now.strftime("%Y%m%d%H%M%S")

       

         # Initialize temporary CSV file to store crawled data
        self.temp_csv_filename_image = f'ttemp_backupcrawl1_img{timestamp}.csv'
        
        # Check if the file already exists
        file_exists_image = os.path.isfile(self.temp_csv_filename_image)
        
        self.temp_csv_file_image = open(self.temp_csv_filename_image, 'a', newline='')
        self.temp_csv_writer_image = csv.DictWriter(self.temp_csv_file_image, fieldnames=self.fieldnames_image)

        # If the file didn't exist, write the header
        if not file_exists_image:
            self.temp_csv_writer_image.writeheader()

         # Append timestamp to filename
        self.temp_csv_filename = f'ttemp_backupcrawl1_{timestampimg}.csv'
        
        # Check if the file already exists
        file_exists = os.path.isfile(self.temp_csv_filename)
        
        self.temp_csv_file = open(self.temp_csv_filename, 'a', newline='')
        self.temp_csv_writer = csv.DictWriter(self.temp_csv_file, fieldnames=self.fieldnames)

        # If the file didn't exist, write the header
        if not file_exists:
            self.temp_csv_writer.writeheader()

    



    def read_start_urls(self):
        with open('javaurl.csv', 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                if row:  # Skip empty rows
                    self.start_urls.append(row[0])

    def get_country_code(self):
        # Get public IP address
        ip = requests.get('https://api.ipify.org').text
        # Get country code based on IP address
        try:
            response = requests.get(f'http://ip-api.com/json/{ip}').json()
            return response['countryCode']
        except Exception:
            return None
        

    def parse(self, response):
        # Extract links and follow them:
        for link in response.css('a::attr(href)').extract():
            yield response.follow(link, callback=self.parse_item)

    def get_heading_or_paragraph_or_title_text(self, element,website_title):
        headings_and_paragraphs = ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p']
        for tag in headings_and_paragraphs:
            next_tag = element.find_next(tag)
            if next_tag:
                return next_tag.get_text(strip=True)
        return website_title

    

    def parse_item(self, response):
        if response.url in self.processed_urls or response.url.startswith('https://web.archive.org'):
            return

        # Process main URL
        data = {}
        
        
        with ThreadPoolExecutor(max_workers=22) as executor:
            future1 = executor.submit(self.parse_text_content, response, data)
            future2 = executor.submit(self.parse_images, response)

        wait([future1, future2])  # Wait for both tasks to complete

        yield data
    
    def parse_text_content(self, response, data):
        
        g = Goose()
        article = g.extract(raw_html=response.body)

        title = article.title
        description = article.meta_description
        content = self.extract_content(response)
        keywords = self.extract_keywords(description, content)
        pagerank = self.calculate_pagerank(response.url, keywords)

        if not description:
            description = content

        skip_phrases = ["Are you Bot", "You are Blocked","Request Rejected","404 Not Found","Not Found" ,"You're temperory blocked", "blocked","Just a moment...", "Bot","You’re Temporarily Blocked","This website uses cookies"]
        if any(phrase in title for phrase in skip_phrases):
            self.processed_urls.add(response.url)
            return

        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.body, 'html.parser')

        img_tags = soup.find_all('img')

        # Extract all the 'src' or 'data-src' attributes from the 'img' tags
        images = [img.get('src') if img.get('src') is not None else img.get('data-src') for img in img_tags]

        large_images = []
        all_images = []
        for img_url in images:
            try:
                response_img = requests.get(img_url)
                if response_img.status_code == 200:
                    img = Image.open(BytesIO(response_img.content))
                    all_images.append(img_url)  # Save all images
                    width, height = img.size
                    if width >= 80 and height >= 80:
                        large_images.append(img_url)  # Save large images
                else:
                    print(f"Failed to fetch image: {img_url}")
            except Exception as e:
                print(f"Error processing image: {img_url}. {e}")

        # Use the first large image if available, otherwise use the first image of any size
        image_urlnew = large_images[0] if large_images else (all_images[0] if all_images else '')  

        data = {
            'id': str(uuid.uuid4()),
            'url': response.url,
            'title': title,
            'description': description,
            'content': content,
            'keyword': keywords,
            'seo_rank': pagerank,
            'image_url': image_urlnew,
            'country': self.country_code
        }

        

        if not self.document_exists(response.url):
            self.data_list.append(data)
            self.save_backup(data)
            print(*self.data_list)
            print("dataon backup"+str(len(self.data_list)))
            if len(self.data_list) >= 10:  # if the length of data_list reaches 10, save it to Meilisearch
                self.save_to_meilisearch(self.data_list)
                self.data_list.clear()
            


        self.processed_urls.add(response.url)
        self.crawled_count += 1

        if self.crawled_count % 20 == 0:
            self.upload_to_bunnycdn(self.temp_csv_filename)


    def parse_images(self, response):
        soup = BeautifulSoup(response.body, 'html.parser')
        image_tags = soup.find_all('img')

        meta_description_tag = soup.find('meta', attrs={'name': 'description'})
        description = meta_description_tag.get('content') if meta_description_tag else None
        content = self.extract_content(response)

        if not description:
            description = content

        keywords = self.extract_keywords(description, content)


        print(f'Found {len(image_tags)} images')

        try:
            website_title = soup.title.get_text(strip=True) if soup.title else None
        except Exception as e:
            print(f"Error occurred: {e}")
            print(f"Type of soup.title: {type(soup.title)}")
            

        print("Before keywords extraction")
        try:
            keywords = self.extract_keywords(description, content)
        except Exception as e:
            print(f"Error occurred during keyword extraction: {e}")
        print("After keywords extraction")

        print("Before the loop")
        for img in image_tags:
            print("Inside the loop")
            src = img.get('src', '')
            alt = img.get('alt', '')
            image_title = alt.strip() if alt and not alt.lower().startswith('image') else None
            print("Before try block")
            if not image_title:
                heading_or_paragraph_or_title_text = self.get_heading_or_paragraph_or_title_text(img.parent,website_title)
                image_title = heading_or_paragraph_or_title_text if heading_or_paragraph_or_title_text else None
            if src:
                image_url = urljoin(response.url, src)

                try:
                    response_img = requests.get(image_url, timeout=5)
                    img = Image.open(BytesIO(response_img.content))  # Try to open the response content as an image
                    img.verify()  # Verify that the image data is valid

                    width, height = img.size
                    if width < 80 or height < 80:
                        print(f"Image is too small: {image_url}")
                        continue 

                    nsfw_score = is_nsfw(image_url)

                    image_data = {
                        'id': str(uuid.uuid4()),
                        'name': image_title,
                        'imageurl': image_url,
                        'link': response.url,
                        'keywords': keywords,
                        'safe_search': nsfw_score,
                        'country': self.country_code
                    }

                except (requests.RequestException, IOError) as e:
                    print(f"Failed to fetch or open image: {image_url}. {e}")
                    continue 

                self.crawledimg_count += 1

                if self.crawledimg_count % 20 == 0:
                    self.upload_to_bunnycdn_image(self.temp_csv_filename_image)

                if not self.documentimage_exists(image_url):
                    self.image_data_list.append(image_data)
                    self.save_backup_image(image_data)
                    print("dataon img backup"+str(len(self.image_data_list)))
                    if len(self.image_data_list) >= 10:
                        self.save_to_meilisearch_image(self.image_data_list)
                        self.image_data_list.clear() 





   


    def get_backlinks(self, url):
        # Get the HTML of the page.
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all the links on the page.
        links = soup.find_all('a')

        # Extract the href attributes from the links.
        backlinks = []
        for link in links:
            if 'href' in link.attrs:
                backlink = link['href']
                if backlink.startswith('http') and not backlink.endswith('.pdf') and not self.is_main_domain_url(backlink):
                    backlinks.append(backlink)

        return backlinks

    def extract_content(self, response):
        selector = Selector(response)
        content = selector.xpath('//p//text()').getall()
        content = ' '.join(content).strip()
        content = ' '.join(content.split())[:2000]
        return content

    def extract_keywords(self, description, content):
        text = description + ' ' + content
        doc = self.nlp(text)
        keywords = []
        for chunk in doc.noun_chunks:
            if not chunk.text.isnumeric() and len(chunk.text) > 1:
                keywords.append(chunk.text.lower())

        for entity in doc.ents:
            if entity.label_ in ['PERSON', 'ORG', 'GPE']:
                keywords.append(entity.text.lower())

        keywords = [token.lemma_.lower() for token in doc if not token.is_stop and token.is_alpha and len(token.text) > 1]
        return keywords

    def calculate_pagerank(self, url, keywords):
        backlinks = self.get_backlinks(url)
        num_backlinks = len(backlinks)

        if num_backlinks == 0:
            return 0  # or set a default value based on your requirements

        pagerank = 1 / num_backlinks

        for backlink in backlinks:
            pagerank += 1 / num_backlinks

        for keyword in keywords:
            if keyword in url:
                pagerank += 1

        pagerank *= 0.85
        return pagerank

    def is_main_domain_url(self, url):
        domain = tldextract.extract(url).registered_domain
        main_domain = tldextract.extract(self.start_urls[0]).registered_domain
        return domain == main_domain

    def documentimage_exists(self,url):
        headers = {
            'Authorization': 'Bearer ZGM1NDcxM2JmNzhhZjFlYTFhNTZkZDVh',
        }

        body = {
            "q": url
        }

        response = requests.post("https://searchapiv2.aksharakuppy.com/indexes/newsearch_images/search", headers=headers, json=body)
        
        if response.status_code == 200:
            response_json = response.json()

            for hit in response_json['hits']:
                if hit['imageurl'] == url:
                    return True

        return False

    def document_exists(self, url):
        headers = {
            'Authorization': 'Bearer ZGM1NDcxM2JmNzhhZjFlYTFhNTZkZDVh',
        }
        body = {
            "q": url
        }

        response = requests.post("https://searchapiv2.aksharakuppy.com/indexes/newsearch_results/search", headers=headers, json=body)

        if response.status_code == 200:
            response_json = response.json()
            for hit in response_json['hits']:
                if hit['url'] == url:
                    return True

        return False

    def save_to_meilisearch(self, data):
        headers = {
            'Authorization': 'Bearer ZGM1NDcxM2JmNzhhZjFlYTFhNTZkZDVh',
            'Content-Type': 'application/json',
        }
        url = "https://searchapiv2.aksharakuppy.com/indexes/newsearch_results/documents"
        payload = json.dumps(data)
        response = httpx.post(url, headers=headers, data=payload)
        print("Response Status Code:", response.status_code)
        print("Response Content:", response.content)
       # print(f"Data for URL '{data['url']}' inserted into Meilisearch.")

    def save_backup(self, data):
        # Write the data dictionary to the temporary CSV file
        self.temp_csv_writer.writerow(data)

    def save_to_meilisearch_image(self, image_data_list):
        headers = {
            'Authorization': 'Bearer ZGM1NDcxM2JmNzhhZjFlYTFhNTZkZDVh',
            'Content-Type': 'application/json',
        }
        url = "https://searchapiv2.aksharakuppy.com/indexes/newsearch_images/documents"
        payload = json.dumps(image_data_list)  # Notice that data is now a list
        response = httpx.post(url, headers=headers, data=payload)
        print("Response Status Code:", response.status_code)
        print("Response Content:", response.content)



    def save_backup_image(self, data):
        # Write the image data dictionary to the temporary CSV file
        self.temp_csv_writer_image.writerow(data)

    def upload_to_bunnycdn_image(self, filename):
        username = 'aksharasearch'
        hostname = 'sg.storage.bunnycdn.com'
        password = 'b81e0f4a-ca88-48ff-a39929053410-65d7-4768'
        port = 21
        directory = '/aksharasearch/aksearch_images'
        ftp = FTP()
        ftp.connect(hostname, port)
        ftp.login(username, password)
        ftp.cwd(directory)
        with open(filename, 'rb') as f:
            ftp.storbinary(f'STOR {os.path.basename(filename)}', f)
        ftp.quit()
        try:
            os.remove(filename)
        except OSError as e:
            print(f"Error: {e.filename} - {e.strerror}")

    def upload_to_bunnycdn(self, filename):
        username = 'aksharasearch'
        hostname = 'sg.storage.bunnycdn.com'
        password = 'b81e0f4a-ca88-48ff-a39929053410-65d7-4768'
        port = 21
        directory = '/aksharasearch/aksearch_results'
        ftp = FTP()
        ftp.connect(hostname, port)
        ftp.login(username, password)
        ftp.cwd(directory)
        with open(filename, 'rb') as f:
            ftp.storbinary(f'STOR {os.path.basename(filename)}', f)
        ftp.quit()
        try:
            os.remove(filename)
        except OSError as e:
            print(f"Error: {e.filename} - {e.strerror}")

    def closed(self, reason):
        # Close the temporary CSV file before the spider is closed
        self.temp_csv_file.close()
        self.temp_csv_file_image.close()


# Run the spider
if __name__ == "__main__":
    from scrapy.crawler import CrawlerProcess
    process = CrawlerProcess()
    process.crawl(MySpider)
    process.start()
