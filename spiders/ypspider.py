# -*- coding: utf-8 -*-
import scrapy
import math
import json
import time
from bs4 import BeautifulSoup
from yp.postgresdb import dbmanager
from scrapy import signals
from scrapy.exceptions import DontCloseSpider


base_url = "https://www.yellowpages.com"


def get_substring(instr, startstr, endstr=None):
    """
    Get sub string between the start and end string
    """
    if startstr == "":
        length = instr.find(endstr)
        if length >= 0:
            return instr[:length]

    start_idx = instr.find(startstr)
    if start_idx == -1:
        return ""
    start_idx += len(startstr)
    if start_idx >= 0:
        if endstr:
            length = instr[start_idx:].find(endstr)
            if length == -1:
                return instr[start_idx:]                            
            return instr[start_idx:start_idx+length]
        return instr[start_idx:] 


class YpspiderSpider(scrapy.Spider):
    name = 'ypspider'
    # allowed_domains = ['yellowpages.com']
    start_urls = ['https://yellowpages.com/']

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(YpspiderSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_idle, signal=signals.spider_idle)
        return spider

    def spider_idle(self):
        """Schedules a request if available, otherwise waits."""
        print ("========== SPIDER IDLE ==========")
        request = self.schedule_next_requests()
        # if requests:
        #     for request in requests:
        self.crawler.engine.crawl(request, self)
        print ("========== SPIDER CREATE REQUEST ==========")
    
        raise DontCloseSpider        	


    def schedule_next_requests(self):

        """Schedules a request if available"""

        result = dbmanager.fetchLink()
        linkInfo = result[0]
        category = {
            "url":              linkInfo['url'],
            "main_category":    linkInfo['main_category'],
            "sub_category":     linkInfo['sub_category'],
            "state":            linkInfo['state'],
            "city":             linkInfo['city']
        }
        print ("=====>>>>>Category to crawl :", category)

        request = scrapy.Request(url=category['url'], callback=self.get_category_page)
        request.meta['category'] = category
        return request


    # def parse(self, response):
    #     link_count = dbmanager.getLinkCount()

    #     for i in range(link_count+1):
    #         result = dbmanager.fetchLink()
    #         linkInfo = result[0]
    #         category = {
    #             "url":              linkInfo['url'],
    #             "main_category":    linkInfo['main_category'],
    #             "sub_category":     linkInfo['sub_category'],
    #             "state":            linkInfo['state'],
    #             "city":             linkInfo['city']
    #         }
    #         print ("=====>>>>>Category to crawl :", category)

    #         request = scrapy.Request(url=category['url'], callback=self.get_category_page)
    #         request.meta['category'] = category
    #         yield request


    def start_requests(self):
        result = dbmanager.fetchLink()
        linkInfo = result[0]
        category = {
            "url":              linkInfo['url'],
            "main_category":    linkInfo['main_category'],
            "sub_category":     linkInfo['sub_category'],
            "state":            linkInfo['state'],
            "city":             linkInfo['city']
        }
        print ("=====>>>>>Category to crawl :", category)

        request = scrapy.Request(url=category['url'], callback=self.get_category_page)
        request.meta['category'] = category
        yield request
         

    def get_category_page(self, response):
        category = response.meta['category']
        soup = BeautifulSoup(response.text, 'lxml')

        page_count = 0
        doc_count_item = soup.select_one("div.pagination > p")
        if doc_count_item:
            doc_count = doc_count_item.text.strip()
            doc_count = get_substring(doc_count, "found", "result")
            page_count = math.floor(int(doc_count) / 30) + 1
            # print ("===>>> Page Count =", page_count, doc_count)

            business_items = soup.select("a.business-name")
            for business_item in business_items:
                business_url = base_url + business_item['href']

                request = scrapy.Request(url=business_url, callback=self.get_business_page)
                request.meta['category'] = category
                yield request

            # return
            # crawl next pages
            for i in range(2, page_count+1):
                page_url = "{}?page={}".format(response.url, i)

                request = scrapy.Request(url=page_url, callback=self.get_category_nextpage)
                request.meta['category'] = category
                yield request


    def get_category_nextpage(self, response):
        category = response.meta['category']
        soup = BeautifulSoup(response.text, 'lxml')

        business_items = soup.select("a.business-name")
        for business_item in business_items:
            business_url = base_url + business_item['href']

            request = scrapy.Request(url=business_url, callback=self.get_business_page)
            request.meta['category'] = category
            yield request


    def get_business_page(self, response):
        category = response.meta['category']
        soup = BeautifulSoup(response.text, 'lxml')

        header = soup.find("header", {"id": "main-header"})

        # name = ""
        # name_item = header.find("div", {"class": "sales-info"})
        # if name_item:
        #     name = name_item.text.strip()

        # address = ""
        # address_item = header.select_one("div.contact > h2.address")
        # if address_item:
        #     address = address_item.text.strip()

        # phone = ""
        # phone_item = header.select_one("div.contact > p.phone")
        # if phone_item:
        #     phone = phone_item.text.strip()

        # website = ""
        # website_item = header.find("a", {"class": "website-link"})
        # if website_item:
        #     website = website_item.text.strip()

        # email = ""
        # email_item = header.find("a", {"class": "email-business"})
        # if email_item:
        #     email = email_item.text.strip()

        business_years = ""
        years_item = header.find("div", {"class": "number"})
        if years_item:
            business_years = years_item.text.strip()

        # application+json
        json_item = soup.find("script", {"type": "application/ld+json"})
        if json_item == None:
            return

        json_txt = json_item.text.strip()
        business_obj = json.loads(json_txt)
        if '@id' not in business_obj:
            return
        if 'name' not in business_obj:
            return
        
        # business info
        business_information = ""
        business_info = {}
        business_info_item = soup.find("section", {"id": "business-info"})
        if business_info_item:

            business_info['slogan'] = ""
            slogan_item = business_info_item.find("h2", {"class": "slogan"})
            if slogan_item:
                business_info['slogan'] = slogan_item.text.strip()
            
            info_headers = business_info_item.select("dt")
            info_contents = business_info_item.select("dd")
            for header, content in zip(info_headers, info_contents):
                header = header.text.strip()
                content = content.text.strip()
                business_info[header] = content
            
            business_information = json.dumps(business_info)

        # gallery
        gallery = ""
        galleries = []
        gallery_items = soup.select("div.collage-item > a")
        for item in gallery_items:
            galleries.append(base_url + item['href'])
            gallery = ','.join(galleries)

        description = ""
        if 'location' in business_obj:
            if 'description' in business_obj['location']:
                description = business_obj['location']['description']

        country = ""
        street = ""
        locality = ""
        region = ""
        postalCode = ""
        if 'address' in business_obj:
            if 'addressCountry' in business_obj['address']:
                country = business_obj['address']['addressCountry']
            if 'streetAddress' in business_obj['address']:
                street = business_obj['address']['streetAddress']
            if 'addressLocality' in business_obj['address']:
                locality = business_obj['address']['addressLocality']
            if 'addressRegion' in business_obj['address']:
                region = business_obj['address']['addressRegion']
            if 'postalCode' in business_obj['address']:
                postalCode = business_obj['address']['postalCode']

        telephone = ""
        if 'telephone' in business_obj:
            telephone = business_obj['telephone']

        latitude = ""
        longitude = ""
        if 'geo' in business_obj:
            latitude = str(business_obj['geo']['latitude'])
            longitude = str(business_obj['geo']['longitude'])

        opening_hours = ""
        if 'openingHours' in business_obj:
            opening_hours = ','.join(business_obj['openingHours'])

        review = ""
        if 'review' in business_obj:
            if len(business_obj['review']) > 0:
                review = json.dumps(business_obj['review'])

        image = ""
        if 'image' in business_obj:
            if 'url' in business_obj['image']:
                image = business_obj['image']['url']
        
        website = ""
        if 'url' in business_obj:
            website = business_obj['url']

        email = ""
        if 'email' in business_obj:
            email = business_obj['email']
            email = get_substring(email, "mailto:")

        payment = ""
        if 'paymentAccepted' in business_obj:
            payment = business_obj['paymentAccepted']

        document = {
            'id'            : business_obj['@id'],
            'name'          : business_obj['name'],
            'description'   : description,
            'main_category' : category['main_category'],
            'sub_category'  : category['sub_category'],
            'country'       : country,
            'street'        : street,
            'locality'      : locality,
            'region'        : region,
            'postalcode'    : postalCode,
            'latitude'      : latitude,
            'longitude'     : longitude,
            'telephone'     : telephone,
            'business_years': business_years,
            'opening_hours' : opening_hours,
            'image'         : image,
            'website'       : website,
            'email'         : email,
            'payment'       : payment,
            'information'   : business_information,
            'gallery'       : gallery,
            'review'        : review,
            "url"           : response.url
        }

        print ("Business====================", document['name'], document['id'])
        # print (document)

        dbmanager.insertBusiness(document)
