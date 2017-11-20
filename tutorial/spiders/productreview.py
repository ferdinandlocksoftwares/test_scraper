# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
import json
import re
import os


class ProductreviewSpider(scrapy.Spider):
	name = "productreview"
	#declaration for allowed domains, httpstatus and db connection
	# allowed_domains = ['amazon.com','amazon.co.uk','amazon.de','amazon.fr', 'amazon.it','amazon.ca', 'amazon.es']
	handle_httpstatus_list = [404, 301, 302, 303, 307]
	handle_httpstatus_all = True
	dir_path = ''

	#declaration for variable used
	country = None
	asin_list = None
	seller_id = None

	def url_to_country(self, url):
		c = url.split('.')[-1]
		if c == 'com':
			return 'us'
		else:
			return c

	def __init__(self, *args, **kwargs):
		super(ProductreviewSpider, self).__init__(*args, **kwargs)
		self.country = str(self.cty).lower()
		self.seller_id = str(self.sid)
		if self.country == 'gb':
			self.country = 'uk'
		self.asin_list = str(self.asin).split(',')

	def start_requests(self):
		for asin in self.asin_list:
			# start parsing store fronts
			if self.country == 'uk':
				url = "https://www.amazon.co.uk/product-reviews/"+asin+"/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&pageSize=100&sortBy=recent&pageNumber=1"
			elif self.country == 'us':
				url = "https://www.amazon.com/product-reviews/"+asin+"/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&pageSize=100&sortBy=recent&pageNumber=1"
			else:
				url = "https://www.amazon."+self.country+"/product-reviews/"+asin+"/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&pageSize=100&sortBy=recent&pageNumber=1"

			yield Request(url, callback=self.parse_product_reviews)

	def parse_product_reviews(self, response):
		if response.status == 200:
			if response.css('div#cm_cr-review_list.review-views'):
				for review in response.css('div#cm_cr-review_list.review-views div.review'):
					yield {
						"seller_id" : self.seller_id,
						"review_code" : review.css('::attr(id)').extract_first(),
						"asin" : response.url.split('/')[4],
						"country" : self.country,
						"star" : review.css('i.review-rating span.a-icon-alt::text').extract_first().split(' ')[0].replace(',', '.'),
						"review_title" : review.css('a.review-title::text').extract_first(),
						"author" : review.css('a.author::text').extract_first(),
						"review_date" : review.css('span.review-date::text').extract_first(),
						"variation" : review.css('div.review-data.review-format-strip a::text').extract_first(),
						"verified_purchase" : review.css('span.a-declarative a span::text').extract_first(),
						"review_text" : review.css('span.review-text::text').extract_first(),
						"author_url" : response.urljoin(review.css('a.author::attr(href)').extract_first())
					}

				next_page = response.css('li.a-last a::attr(href)').extract_first()
				if next_page is not None:
					new_url = response.url.split('=')
					rr = new_url[-1]
					rr = int(rr)
					rr += 1
					new_url[-1] = str(rr)
					next_page = '='.join(new_url)
					yield response.follow(next_page, callback=self.parse_product_reviews)

		else:
			if response.status == 404:
				self.response_404(response)

			elif response.status in [301, 302, 303, 307]:
				self.response_redirect(response)

	def response_404(self, response):
		yield {
			'404-products' : {
				'asin' : response.url.split('/')[-1],
				'country' : self.url_to_country(response.url.split('/')[2]).lower()
			}
		}

	def response_redirect(self, response):
		yield {
			'redirected_url' : {
				'asin' : response.url.split('/')[-1],
				'country' : self.url_to_country(response.url.split('/')[2]).lower(),
				'response_status' : response.status,
				'url' : response.url
			}
		}