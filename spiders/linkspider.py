# -*- coding: utf-8 -*-
import scrapy
import datetime
from bs4 import BeautifulSoup
import re
import time
import string

from yp.postgresdb import dbmanager


base_url = "https://www.yellowpages.com"

class LinkspiderSpider(scrapy.Spider):
    name = 'linkspider'
    # allowed_domains = ['yellowpages.com']
    start_urls = ['https://www.yellowpages.com/sitemap']

    def parse(self, response):
        soup = BeautifulSoup(response.text, 'lxml')

        states_info = []
        row_items = soup.select("div.row-content > div.row")
        for row in row_items:
            h2_item = row.find("h2")
            if h2_item:
                h2 = h2_item.text.strip()
                if h2 == "Local Yellow Pages":
                    state_link_items = row.select("li > a")
                    for state_link in state_link_items:
                        state_name = state_link.text.strip()
                        state_info = {
                            "state": state_name,
                            "url": state_link['href']
                        }
                        states_info.append(state_info)

        alphabet_list = list(string.ascii_lowercase)
        for state_info in states_info:
            for alphabet in alphabet_list:
                state_page_url = "{}/{}?page={}".format(base_url, state_info['url'], alphabet)

                request = scrapy.Request(url=state_page_url, callback=self.get_state_page)
                request.meta['state'] = state_info['state']
                yield request
                # time.sleep(10)

                # return


    def get_state_page(self, response):
        state = response.meta['state']
        
        soup = BeautifulSoup(response.text, 'lxml')

        cities_info = []
        section_columns = soup.findAll("section", {"class": "column"})
        for section in section_columns:
            city_link_items = section.select("a")
            for city_link_item in city_link_items:
                city_name = city_link_item.text.strip()
                city_info = {
                    "city": city_name,
                    "url": city_link_item['href']
                }
                cities_info.append(city_info)

            for city_info in cities_info:
                city_url = base_url + city_info['url']

                request = scrapy.Request(url=city_url, callback=self.get_city_page)
                request.meta['state'] = state
                request.meta['city'] = city_info['city']
                yield request
                # time.sleep(5)
                # return


    def get_city_page(self, response):
        state = response.meta['state']
        city = response.meta['city']

        soup = BeautifulSoup(response.text, 'lxml')

        main_categories = soup.select("section.popular-cats > article")
        for main_category in main_categories:
            main_cat_name_item = main_category.find("h3")
            main_cat_name = main_cat_name_item.text.strip()

            categories = main_category.select("a")
            for category in categories:
                cat_name = category.text.strip()
                cat_link = base_url + category['href']

                cat_info = {
                    "url": cat_link,
                    "main_category": main_cat_name,
                    "sub_category": cat_name,
                    "state": state,
                    "city": city
                }

                dbmanager.insertLink(cat_info)
                # time.sleep(1)
