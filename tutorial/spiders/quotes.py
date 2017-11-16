# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
import MySQLdb
import html
import json
import re
import os


class QuotesSpider(scrapy.Spider):
	name = "quotes"
	#declaration for allowed domains, httpstatus and db connection
	allowed_domains = ['amazon.com','amazon.co.uk','amazon.de','amazon.fr', 'amazon.it','amazon.ca', 'amazon.es']
	handle_httpstatus_list = [404, 301, 302, 303, 307]
	handle_httpstatus_all = True
	host = '192.168.1.208'
	user = 'trendle'
	password = '123'
	db2 = 'trendledbseller'
	dbmain = 'trendledbmain'
	dir_path = '/var/www/public/trendle/public/app/ads_ajax_files/'

	#declaration for variable used
	seller_id = None
	country = ""
	sellers = None
	mkp = None

	def url_to_country(self, url):
		c = url.split('.')[-1]
		if c == 'com':
			return 'us'
		else:
			return c

	def __init__(self, *args, **kwargs):
		super(QuotesSpider, self).__init__(*args, **kwargs)
		self.seller_id = str(self.sid)
		self.mkp = str(self.mkp_id)

		# get seller list and country
		self.connection2 = MySQLdb.connect(self.host, self.user, self.password, self.dbmain)
		self.cursor2 = self.connection2.cursor()
		query_string = "select mkp_a.seller_id, c.iso_3166_2 AS country, mkp_a.marketplace_id FROM marketplace_assigns AS mkp_a"
		query_string += " LEFT JOIN marketplace_countries AS mkp_c ON mkp_a.marketplace_id=mkp_c.marketplace_id"
		query_string += " LEFT JOIN countries AS c ON mkp_c.country_id = c.id"
		query_string += " WHERE mkp_a.seller_id='"+self.seller_id+"' and mkp_a.marketplace_id='"+self.mkp+"'  ORDER BY mkp_a.marketplace_id"
		self.cursor2.execute(query_string)
		self.sellers = self.cursor2.fetchall()
		self.cursor2.close()

		#sets a connection for seller db
		#self.create_cursor_db2_connnection()

	def create_cursor_db2_connnection(self):
		self.connection = MySQLdb.connect(self.host, self.user, self.password, self.db2)
		self.cursor = self.connection.cursor()

	def start_requests(self):
		for seller in self.sellers:
			self.seller_id = str(seller[0])
			self.country  = seller[1].lower()
			self.country = self.country.lower()
			self.mkp = str(seller[2])
			# change the country from gb to uk
			if self.country == 'gb':
				self.country = 'uk'
			#remove the product reviews file of seller-country if exists
			if os.path.exists(self.dir_path+'product-reviews-'+self.seller_id+'-'+self.country+'.json'):
				os.remove(self.dir_path+'product-reviews-'+self.seller_id+'-'+self.country+'.json')
			#remove the bad-asins or (404 response) of the seller-country if exists
			if os.path.exists(self.dir_path+'404-asin'+self.seller_id+'-'+self.country+'.json'):
				os.remove(self.dir_path+'bad-asin'+self.seller_id+'-'+self.country+'.json')
			#remove the captcha url of the seller-country if exists
			if os.path.exists(self.dir_path+'301-url'+self.seller_id+'-'+self.country+'.json'):
				os.remove(self.dir_path+'bad-asin'+self.seller_id+'-'+self.country+'.json')

			self.create_cursor_db2_connnection()
			self.cursor.execute("SELECT asin FROM products where seller_id='"+str(self.seller_id)+"' and country='"+self.country+"' and isactive='1' group by asin")
			links = self.cursor.fetchall()
			for link in links:
				self.asin = str(link[0])

				if self.country == 'uk':
					url = "https://www.amazon.co.uk/product-reviews/dp/"
				elif self.country == 'us':
					url = "https://www.amazon.com/product-reviews/dp/"
				else:
					url = "https://www.amazon."+self.country+"/product-reviews/dp/"

				yield Request(url+ "" + str(link[0]), callback=self.parse)

		self.cursor.close()


	def parse(self, response):
		#success status
		if response.status == 200:
			if len(response.url.split('/')) == 6:
				# see all part
				features = response.css('div#feature-bullets').extract_first()
				features = re.sub('\t', '', str(features)).strip()
				# features = re.sub('\n', '', features).strip()
				features = re.sub('  ', '', features).strip()
				p_alt_images = []
				for alt_img in response.css('div#altImages ul li.item'):
					p_alt_images.append(alt_img.css('img::attr(src)').extract_first())

				prod_title = response.css('div#title_feature_div div#titleSection h1#title span#productTitle::text').extract()
				s = str(''.join(prod_title))
				prod_title = re.sub('\n', '', s).strip()

				main_image_url = response.css('div#imgTagWrapperId img#landingImage.a-dynamic-image::attr(data-old-hires)').extract_first()

				desc = response.css('div#productDescription').extract_first()
				desc = re.sub('\t', '', str(desc)).strip()
				# desc = re.sub('\n', '', str(desc)).strip()
				desc = re.sub('  ', '', desc).strip()
				description = desc
				
				product_rating = response.css('div#reviewSummary a span.a-icon-alt::text').extract_first()
				if product_rating is not None:
					product_rating = product_rating.split(' ')[0].replace(',','.')
					product_rating = str(product_rating)
					product_rating = product_rating.replace(',', '.')

				nb_of_reviews = response.css('div#reviewSummary a span.totalReviewCount::text').extract_first()

				if prod_title == "":
					# products that are already disabled/inactive
					self.response_404(response)
				else:
					# active products
					# save thier details in a .json file
					review_dict =  {
						'asin' : response.url.split('/')[-1],
						'country' : self.url_to_country(response.url.split('/')[2]).lower(),
						'product_title' : prod_title,
						'product_description' : description,
						'product_features' : features,
						'product_landing_image_url': main_image_url,
						'product_alt_image_url' : p_alt_images,
						'product_rating': product_rating,
						'nb_of_reviews' : nb_of_reviews
					}
					parse_product_data(response, review_dict)
					


			if response.css('div#reviewSummary a#dp-summary-see-all-reviews::attr(data-hook)').extract_first() == 'see-all-reviews-link':
			# if response.css('div#reviews-medley-footer div a::attr(data-hook)').extract_first() == 'see-all-reviews-link-foot':
				see_all = response.css('div#reviewSummary a#dp-summary-see-all-reviews::attr(href)').extract_first()
				# see_all = response.css('div#reviews-medley-footer div a::attr(href)').extract_first()

				# open a connection for getting the number of reviews in a specific product
				self.create_cursor_db2_connnection();
				self.cursor.execute("SELECT IFNULL(nb_of_reviews,0) FROM product_reviews_products where seller_id='"+str(self.seller_id)+"' and country='"+self.url_to_country(response.url.split('/')[2]).lower()+"' and product_asin='"+response.url.split('/')[-1]+"'")
				nb_q = self.cursor.fetchall()
				nb = 0
				if len(nb_q) > 0:
					nb = nb_q[0][0]
				# close the cursor connection
				self.cursor.close()

				# check if number of reviews increase
				if nb < int(nb_of_reviews):
					#yield/go to link for all reviews 
					next_page = response.urljoin(see_all)
					yield scrapy.Request(next_page, callback=self.parse)

			elif response.css('div.review'):	
				self.parse_product_reviews(response)

				#yield/go to link for the next page
				next_page = response.css('li.a-last a::attr(href)').extract_first()
				if next_page is not None:
					next_page = response.urljoin(next_page)
					yield scrapy.Request(next_page, callback=self.parse)

		#unsuccessful status
		else:
			if response.status == 404:
				self.response_404(response)

			elif response.status in [301, 302, 303, 307]:
				self.response_redirect(response)

	def parse_product_data(self, response, review_dict):
		r = json.dumps(review_dict)
		filename = os.path.join(self.dir_path, 'product-info-'+str(self.seller_id)+'-'+self.url_to_country(response.url.split('/')[2]).lower()+'.json')
		with open(filename, 'a+') as f:
			f.write(r+'\n')
			f.close()

	def parse_product_reviews(self, response):
		for quote in response.css('div.review'):
			url_asin = response.url.split('/')[4]
			if url_asin == 'product-reviews':
				url_asin = response.url.split('/')[5]
			review_dict =  {
				'asin' : url_asin,
				'country' : self.url_to_country(response.url.split('/')[2]).lower(),
		        'review_code': quote.css('::attr(id)').extract_first(),
		        'star': quote.css('span.a-icon-alt::text').extract_first().split(' ')[0],
		        'review_title': quote.css('a.review-title::text').extract_first(),
		        'author': quote.css('a.author::text').extract_first(),
		        'review_date': quote.css('span.review-date::text').extract_first(),
		        'verified_purchase': quote.css('span.a-declarative a span.a-text-bold::text').extract_first(),
		    	'review_text': quote.css('span.review-text::text').extract_first(),
		    	'author_url': response.urljoin(quote.css('a.author::attr(href)').extract_first())
		    }

			r = json.dumps(review_dict)
			filename = os.path.join(self.dir_path, 'product-reviews-'+str(self.seller_id)+'-'+self.url_to_country(response.url.split('/')[2]).lower()+'.json')
			with open(filename, 'a+') as f:
				f.write(r+'\n')
				f.close()

	def response_404(self, response):
		filename = os.path.join(self.dir_path, '404-asin-'+str(self.seller_id)+'-'+self.url_to_country(response.url.split('/')[2]).lower()+'.json')
		with open(filename, 'a+') as f:
			review_dict = {
				'asin' : response.url.split('/')[-1],
				'country' : self.url_to_country(response.url.split('/')[2]).lower()
			}
			r = json.dumps(review_dict)
			f.write(r+'\n')
			f.close()

	def response_redirect(self, response):
		filename = os.path.join(self.dir_path, '301-url-'+str(self.seller_id)+'-'+self.url_to_country(response.url.split('/')[2]).lower()+'.json')
		with open(filename, 'a+') as f:
			review_dict = {
				'asin' : response.url.split('/')[-1],
				'country' : self.url_to_country(response.url.split('/')[2]).lower(),
				'response_status' : response.status,
				'url' : response.url
			}
			r = json.dumps(review_dict)
			f.write(r+'\n')
			f.close()