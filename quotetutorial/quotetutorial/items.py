# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class JobItem(scrapy.Item):
    job_name = scrapy.Field()
    company = scrapy.Field()
    salary = scrapy.Field()
    address = scrapy.Field()
    cv_deadline = scrapy.Field()
    benefits = scrapy.Field()
