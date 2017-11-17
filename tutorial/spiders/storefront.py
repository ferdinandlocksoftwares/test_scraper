# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
import json
import re
import os


class StorefrontSpider(scrapy.Spider):
	name = "storefront"
	#declaration for allowed domains, httpstatus and db connection
	# allowed_domains = ['amazon.com','amazon.co.uk','amazon.de','amazon.fr', 'amazon.it','amazon.ca', 'amazon.es']
	handle_httpstatus_list = [404, 301, 302, 303, 307]
	handle_httpstatus_all = True
	dir_path = ''

	#declaration for variable used
	seller_id = None
	sellers_country = None
	mkp = None
	mws_seller_id = None

	def url_to_country(self, url):
		c = url.split('.')[-1]
		if c == 'com':
			return 'us'
		else:
			return c

	def __init__(self, *args, **kwargs):
		super(StorefrontSpider, self).__init__(*args, **kwargs)
		self.seller_id = str(self.sid)
		self.mkp = str(self.mkp_id)
		self.mws_seller_id = str(self.msid)

		if self.mkp == '1':
			self.sellers_country = ['us', 'ca']
		else:
			self.sellers_country = ['uk', 'de', 'es', 'it', 'fr']

	def start_requests(self):
		for seller in self.sellers_country:
			self.country  = seller.lower()
			# change the country from gb to uk
			if self.country == 'gb':
				self.country = 'uk'
			
			# start parsing store fronts
			if self.country == 'uk':
				url = "https://www.amazon.co.uk/s/ref=sr_pg_1?me="+self.mws_seller_id
			elif self.country == 'us':
				url = "https://www.amazon.com/s/ref=sr_pg_1?me="+self.mws_seller_id
			else:
				url = "https://www.amazon."+self.country+"/s/ref=sr_pg_1?me="+self.mws_seller_id

			yield Request(url, callback=self.parse_store_front)

	def parse_store_front(self, response):
		if response.status == 200:
			for product in response.css('div#atfResults ul#s-results-list-atf li.s-result-item'):
				asin = product.css('::attr(data-asin)').extract_first()
				country = self.url_to_country(response.url.split('/')[2]).lower()
				nb_of_reviews = product.css('div.s-item-container div.a-row.a-spacing-none a::text').extract()
				nb_of_reviews = nb_of_reviews[-1]
				product_rating = product.css('div.s-item-container div.a-row.a-spacing-none span span a span.a-icon-alt::text').extract_first().split(' ')[0]
				prod_title = product.css('div.s-item-container div div a.s-access-detail-page::attr(title)').extract_first()
				yield {
					'seller_id' : self.seller_id,
					'asin' : asin,
					'country' : country,
					'product_title' : prod_title,
					'product_rating': product_rating.replace(',', '.'),
					'nb_of_reviews' : nb_of_reviews
				}

			next_page = response.css('span.pagnRa a#pagnNextLink.pagnNext::attr(href)').extract_first()
			if next_page is not None:
				next_page = response.urljoin(next_page)
				yield scrapy.Request(next_page, callback=self.parse_store_front)