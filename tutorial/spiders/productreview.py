# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
import json
import re
import os


class ProductreviewSpider(scrapy.Spider):
	name = "productreview"
	#declaration for allowed domains, httpstatus and db connection
	allowed_domains = ['amazon.com','amazon.co.uk','amazon.de','amazon.fr', 'amazon.it','amazon.ca', 'amazon.es']
	handle_httpstatus_list = [404, 301, 302, 303, 307]
	handle_httpstatus_all = True
	dir_path = ''

	#declaration for variable used
	seller_id = None
	country = ""
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
		super(ProductreviewSpider, self).__init__(*args, **kwargs)
		self.seller_id = str(self.sid)
		self.mkp = str(self.mkp_id)
		self.mws_seller_id = str(self.msid)

		if self.mkp == 1:
			self.sellers_country = ['us', 'ca']
		else:
			self.sellers_country = ['uk', 'de', 'es', 'it', 'fr']

	# def create_cursor_db2_connnection(self):
	# 	self.connection = MySQLdb.connect(self.host, self.user, self.password, self.db2)
	# 	self.cursor = self.connection.cursor()

	def start_requests(self):
		for seller in self.sellers_country:
			self.country  = seller.lower()
			# change the country from gb to uk
			if self.country == 'gb':
				self.country = 'uk'
			# #remove the product reviews file of seller-country if exists
			# if os.path.exists(self.dir_path+'product-reviews-'+self.seller_id+'-'+self.country+'.json'):
			# 	os.remove(self.dir_path+'product-reviews-'+self.seller_id+'-'+self.country+'.json')
			# #remove the bad-asins or (404 response) of the seller-country if exists
			# if os.path.exists(self.dir_path+'404-asin'+self.seller_id+'-'+self.country+'.json'):
			# 	os.remove(self.dir_path+'bad-asin'+self.seller_id+'-'+self.country+'.json')
			# #remove the captcha url of the seller-country if exists
			# if os.path.exists(self.dir_path+'301-url'+self.seller_id+'-'+self.country+'.json'):
			# 	os.remove(self.dir_path+'bad-asin'+self.seller_id+'-'+self.country+'.json')
			
			# start parsing store fronts
			if self.country == 'uk':
				url = "https://www.amazon.co.uk/s/ref=sr_pg_1?me="+self.mws_seller_id
			elif self.country == 'us':
				url = "https://www.amazon.com/s/ref=sr_pg_1?me="+self.mws_seller_id
			else:
				url = "https://www.amazon."+self.country+"/s/ref=sr_pg_1?me="+self.mws_seller_id

			yield Request(url, callback=self.parse_store_front)

	def parse_store_front(self, response):
		# https://www.amazon.com/product-reviews/B00OPF9HQO/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&pageSize=100&sortBy=recent&pageNumber=1
		if response.status == 200:
			for product in response.css('ul#s-results-list-atf li'):
				asin = product.css('::attr(data-asin)').extract_first()
				country = self.url_to_country(response.url.split('/')[2]).lower()
				nb_of_reviews = product.css('div.s-item-container div.a-row.a-spacing-none a::text').extract_first()
				product_rating = product.css('div.s-item-container div.a-row.a-spacing-none span span a span.a-icon-alt::text').extract_first().split(' ')[0]
				prod_title = product.css('div.s-item-container div div a.s-access-detail-page::attr(title)').extract_first()
				# review_dict =  {
				yield {
					'review_product' : {
						'asin' : asin,
						'country' : country,
						'product_title' : prod_title,
						'product_rating': product_rating,
						'nb_of_reviews' : nb_of_reviews
					}
				}
				
				# review_dict.append({'date_of_changed': 1})
				# self.save_product_data(response, review_dict)
				pr_url = "/product-reviews/"+asin+"/ref=cm_cr_arp_d_viewopt_srt?ie=UTF8&reviewerType=all_reviews&pageSize=100&sortBy=recent&pageNumber=1"
				pr_url = response.urljoin(pr_url)
				if (nb_of_reviews is not None) or (nb_of_reviews > 0):
					# start parsing product reviews
					yield scrapy.Request(pr_url, callback=self.parse_product_reviews)

	def parse_product_reviews(self, response):
		if response.status == 200:
			if response.css('div.reviews-content div#cm_cr-review_list'):
				self.save_product_reviews()

				#yield/go to link for the next page
				next_page = response.css('li.a-last a::attr(href)').extract_first()
				if next_page is not None:
					new_url = response.url.split('/')
					new_url[-1] = new_url[-1]+1
					# next_page = response.urljoin(next_page)
					next_page = '/'.join(new_url)
					yield scrapy.Request(next_page, callback=self.parse_product_reviews)

		else:
			if response.status == 404:
				self.response_404(response)

			elif response.status in [301, 302, 303, 307]:
				self.response_redirect(response)

	def save_product_reviews(self, response):
		for review in response.css('div.reviews-content div#cm_cr-review_list div.review'):
			# review_dict = {
			yield {
				"reviews": {
					"review_code" : review.css('::attr(id)').extract_first(),
					"asin" : response.url.split('/')[4],
					"country" : self.url_to_country(response.url.split('/')[2]),
					"star" : review.css('i.review-rating span.a-icon-alt::text').extract_first().split(' ')[0],
					"review_title" : review.css('a.review-title::text').extract_first(),
					"author" : review.css('a.author::text').extract_first(),
					"review_date" : review.css('span.review-date::text').extract_first(),
					"variation" : review.css('div.review-data.review-format-strip a::text').extract_first(),
					"verified_purchase" : review.css('span.a-declarative a span::text').extract_first(),
					"review_text" : review.css('span.review-text::text').extract_first(),
					"author_url" : response.urljoin(review.css('a.author::attr(href)').extract_first())
				}
			}
			# r = json.dumps(review_dict)
			# filename = os.path.join(self.dir_path, 'product-reviews-'+str(self.seller_id)+'-'+self.url_to_country(response.url.split('/')[2]).lower()+'.json')
			# with open(filename, 'a+') as f:
			# 	f.write(r+'\n')
			# 	f.close()

	def save_product_data(self, response, review_dict):
		r = json.dumps(review_dict)
		filename = os.path.join(self.dir_path, 'product-info-'+str(self.seller_id)+'-'+self.url_to_country(response.url.split('/')[2]).lower()+'.json')
		with open(filename, 'a+') as f:
			f.write(r+'\n')
			f.close()

	def response_404(self, response):
		# filename = os.path.join(self.dir_path, '404-asin-'+str(self.seller_id)+'-'+self.url_to_country(response.url.split('/')[2]).lower()+'.json')
		# with open(filename, 'a+') as f:
		# review_dict = {
		yield {
			'404-products' : {
				'asin' : response.url.split('/')[-1],
				'country' : self.url_to_country(response.url.split('/')[2]).lower()
			}
		}
			# r = json.dumps(review_dict)
			# f.write(r+'\n')
			# f.close()

	def response_redirect(self, response):
		# filename = os.path.join(self.dir_path, '301-url-'+str(self.seller_id)+'-'+self.url_to_country(response.url.split('/')[2]).lower()+'.json')
		# with open(filename, 'a+') as f:
		# review_dict = {
		yield {
			'redirected_url' : {
				'asin' : response.url.split('/')[-1],
				'country' : self.url_to_country(response.url.split('/')[2]).lower(),
				'response_status' : response.status,
				'url' : response.url
			}
		}
			# r = json.dumps(review_dict)
			# f.write(r+'\n')
			# f.close()