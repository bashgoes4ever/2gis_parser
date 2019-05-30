# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
from random import choice, uniform
from time import sleep, time
from multiprocessing import Pool
import csv
import re


BASE_URL = 'https://2gis.ru'
BUSINESS_URL = '/spb/search/срубы%20бань/'


def write_href(h):
	with open('hrefs.txt', 'a') as f:
		f.write(h + '\n')


def write_csv(data):
	with open('base.csv', 'a') as f:
		writer = csv.writer(f, delimiter=';', lineterminator='\n')
		writer.writerow((data['title'],data['phone'],data['address'],data['site'],data['email']))	


def get_html(url, useragent=None, proxy=None):
	r = requests.get(url, headers=useragent, proxies=proxy)
	return r.text


# сбор ссылок на страницы компаний
def get_page_hrefs(html): 
	soup = BeautifulSoup(html, 'lxml')
	content_block = soup.find('div', class_='searchResults__content')
	titles = content_block.find_all('h3', class_='miniCard__headerTitle')
	for title in titles:
		href = title.find('a', class_='link').get('href')
		write_href(href)

# парсинг данных компании
def get_company_data(html):
	soup = BeautifulSoup(html, 'lxml')	
	contacs = soup.find('section', class_='_contact')
	try:
		title = soup.find('h1', class_='cardHeader__headerNameText').text.strip()
	except:
		title = ''

	try:
		temp = []
		phones = contacs.find('div', class_='contact__phonesVisible').find_all('bdo', class_='contact__phonesItemLinkNumber')
		for p in phones:
			temp.append(p.text.strip())
		phone = ', '.join(temp)
	except:
		phone = ''

	try:
		address = soup.find('address', class_='card__address').find('a', class_='card__addressLink').text.strip()
	except:
		address = ''

	try:
		site = contacs.find('div', class_='contact__websites').find('a', class_='contact__linkText').text.strip()
	except:
		site = ''
	try:
		email = contacs.find('div', class_='_type_email').find('a', class_='contact__linkText').text.strip()
	except:
		email = ''

	data = {'title': title,
			'phone': phone,
			'address': address,
			'site': site,
			'email': email}

	write_csv(data)


def parse_hard(company): # без мультипроцессинга добавить аргументы: proxies, useragents, и убрать их инициализацию в функции

	delay = uniform(2,4)

	useragents = open('useragents.txt').read().split('\n')
	proxies = open('proxies.txt').read().split('\n')

	url = BASE_URL + company

	proxy = {'http': 'http://' + choice(proxies)}
	useragent = {'User-Agent': choice(useragents)}

	try:
		html = get_html(url, proxy, useragent)
		get_company_data(html)
		print('SUCCESS! company # ' + url + ' parsed.')
		sleep(delay)
	except:
		print('SUCCESS! company # ' + url + ' parsed.')
		parse_hard(company) #, proxy, useragent
		sleep(delay)


def parse_hrefs(page):
	delay = uniform(1,3)

	useragents = open('useragents.txt').read().split('\n')
	proxies = open('proxies.txt').read().split('\n')

	url = BASE_URL + BUSINESS_URL + str(page)
	proxy = {'http': 'http://' + choice(proxies)}
	useragent = {'User-Agent': choice(useragents)}
	try:
		html = get_html(url, proxy, useragent)
		get_page_hrefs(html)
		print('SUCCESS! page # ' + str(page) + ' parsed')
		sleep(delay)
	except:
		print('FAIL! page # ' + str(page) + ' not parsed')
		parse_hrefs(page)
		sleep(delay)	


# получение количества страниц
def get_total_pages(url, proxies, useragents):
	proxy = {'http': 'http://' + choice(proxies)}
	useragent = {'User-Agent': choice(useragents)}
	html = get_html(url, proxy, useragent)
	soup = BeautifulSoup(html, 'lxml')	
	pages_uncut = soup.find('h2', class_='searchResults__headerName').text.strip()
	return round(int(re.findall(r'\d+', pages_uncut)[0]) / 12)


def main():
	useragents = open('useragents.txt').read().split('\n')
	proxies = open('proxies.txt').read().split('\n')
	first_page = BASE_URL + BUSINESS_URL + '1'
	last_page = get_total_pages(first_page, proxies, useragents)
	total_pages_list = [i for i in range(1, int(last_page)+1)]

	# парсинг ссылок на страницы компаний
	with Pool(60) as p:
		p.map(parse_hrefs, total_pages_list)

	# парсинг инфы со страниц компаний	
	companies = open('hrefs.txt').read().split('\n')[:-1]
	with Pool(60) as p:
		p.map(parse_hard, companies)



if __name__ == '__main__':
	started = time()
	main()
	total_time = time() - started
	print(total_time)