import scrapy
import unicodedata
import pymongo
import json
from ..items import JobItem

# MongoDB connection settings
MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'job_database'
collection_name = 'jobs'

class QuoteSpider(scrapy.Spider):
    name = 'jobs'
    start_urls = [
        'https://careerviet.vn/viec-lam/l%E1%BA%ADp-tr%C3%ACnh-vi%C3%AAn-k-trang-1-vi.html',
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = pymongo.MongoClient(MONGO_URI)
        self.db = self.client[DB_NAME]
        self.collection = self.db[collection_name]
        # Open the file in append mode
        self.file = open('C:/programming/PyCharm/ScrapyDemo/quotetutorial/quotetutorial/data/items.json', 'a', encoding='utf-8')

    def closed(self, reason):
        self.client.close()
        # Close the file
        self.file.close()

    def parse(self, response):
        # Extract job details from the current page
        for job in response.css('div.figure'):
            job_item = self.process_job(job)
            yield job_item

        # Handle pagination
        next_page = response.css('li.next-page a::attr(href)').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def process_job(self, job):
        job_name = self.normalize_text(job.css('div.title a.job_link::text').get())
        company = self.normalize_text(job.css('div.caption a.company-name::text').get())
        salary = self.normalize_text(job.css('div.caption div.salary p::text').get())
        address = self.normalize_text(job.css('div.caption div.location ul li::text').get())
        cv_deadline = self.normalize_text(job.css('div.caption div.expire-date p::text').get())
        benefits = [self.normalize_text(benefit) for benefit in job.css('ul.welfare li::text').extract()]

        job_item = JobItem()
        job_item['job_name'] = job_name
        job_item['company'] = company
        job_item['salary'] = salary
        job_item['address'] = address
        job_item['cv_deadline'] = cv_deadline
        job_item['benefits'] = benefits

        # Save item to MongoDB
        self.collection.insert_one(dict(job_item))

        # Save item to JSON file in single line format
        self.file.write(json.dumps(dict(job_item), ensure_ascii=False) + '\n')

        return job_item

    @staticmethod
    def normalize_text(text):
        if text is not None:
            return unicodedata.normalize("NFC", text.strip())
        return None
