from datetime import datetime, timedelta
# import time
# from zoneinfo import ZoneInfo
import requests
from bs4 import BeautifulSoup
import locale
import os, fnmatch, shutil, platform
import pandas as pd
from osgeo import ogr, osr, gdal
import psycopg2
from psycopg2.extras import *
import json
from vgdb_general import send_to_telegram, send_to_teams
# from timezonefinder import TimezoneFinder
# from tzdata import *


def rus_month_genitive_to_nominative(i_string):
    '''
    This function converts all russian month names in a string from genitive to nominative. All words must be lowercase.
    :param i_string: input string containing month names
    :return: output string with month names converted
    '''
    months_genitive = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
    months_nominative = ['январь', 'февраль', 'март', 'апрель', 'май', 'июнь', 'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь']
    for pair in zip(months_genitive, months_nominative):
        i_string = i_string.replace(pair[0], pair[1])
    return i_string


def download_orders(start=datetime(year=2023, month=1, day=1), end=datetime.now(), search_string='Об утверждении Перечня участков недр', folder='rosnedra_auc', bot_info=('token', 'id')):
    '''
    This function downloads license blocks auctions announcement data from www.rosnedra.gov.ru website
    and saves it to the hierarchy of folders. It OVERWRITES the folder given in the 'folder' parameter.
    The idea is that www.rosnedra.gov.ru has the search engine, which accepts a search string and
    gives a list of results, among which you can find pages with official orders about license blocks auctions
    announcements and data attached to it. This data is usually a pdf with scanned order text and excel spreadsheet
    with license blocks coordinates and attributes. Search result is a list of links to document pages, which,
    in case this is an order, may contain links to the desired pdf and excel. If you give the appropriate search
    string to the engine, e.g. 'Об утверждении Перечня участков недр', than the result will mostly contain desired docs,
    but not guaranteed. This function is an attempt to filter, structurize and download the data about license auction
    announcements for the given period of time. \n
    This function requires: requests, datetime, locale, os, fnmatch, shutil, platform, json, \n
    vgdb_general.send_to_telegram. \n
    :param start: this is the start <datetime> threshold from which to start the search/download.
    :param end: this is the end <datetime> threshold until which to start the search/download. The default values for start and end parameters are 1st January 2023 and current date respectively, but you can change it according to your need.
    :param search_string: this is the search string given to the rosnedra search engine. The default value 'Об утверждении Перечня участков недр' is OK for most cases of searching the license auction announcements orders.
    :param folder: this a relative path to the folder where to save the result. The result will contain a number of folders and a logfile with error reports.
    :param bot_info: tuple containing two strings. This is the credentials to use to send log messages to a Telegram chat from a telegram bot. First string is a telegram token of a bot, second string is an id of a chat to send messages to. You can create a bot using @BotFather. To obtain chat id you need to send a message to the bot, then go to https://api.telegram.org/bot<Bot Token>/getUpdates page and look for something like "chat":{"id": 1234567 ...}. The id parameter is the chat id.
    :return:
    This function returns nothing, but it saves the results to the <folder> location. the results are:
    1. a number of folders with names like NN_YYYYMMDD, where NN is a number of the search result, and YYYYMMDD is a
    date of the Rosnedra order. Each folder contains: pdf with scanned order text, excel with license info,
    result_metadata.json with the metadata of this order;
    2. logfile.txt with reports about parsed pages from the search result. This logfile is filled cumulatively,
    so if you make several attempts, no lines are deleted from it.
    '''
    # variable to return the success result
    success = False
    # get the current working directory of the script
    current_directory = os.getcwd()
    # define the datetime format for the logfile
    logdateformat = '%Y-%m-%d %H:%M:%S'
    # create a pathname for the logfile
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    # open the logfile and start a requests session
    with open(log_file, 'a', encoding='utf-8') as logf, requests.Session() as s:

        message = 'AuctionBlocksUpdater: Download data from Rosnedra started!'
        logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
        send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
        # create url string for the main search request
        url = 'https://www.rosnedra.gov.ru/index.fcgi'
        params = {
            'page': 'search',
            'step': '1',
            'q': search_string
        }
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ru - RU, ru;q = 0.9, en - US;q = 0.8, en;q = 0.7, en - GB;q = 0.6',
            'Connection': 'keep-alive',
            'DNT': '1'
        }
        # try to retreive the result from the search engine
        try:
            # make get request to the service
            search_result = s.get(url, params=params, headers=headers, verify=False)
            # i is a number of tries to retrieve a result
            i = 1
            # while the service returns anything other than 200 (OK):
            while search_result.status_code != 200:
                # make one more try
                search_result = s.get(url, params=params, headers=headers, verify=False)
                i += 1
                # until 100 attempts
                if i > 100:
                    break
        except:
            # if something went wrong, write a line to ligfile
            message = 'AuctionBlocksUpdater: Initial request to www.rosnedra.gov.ru failed, please check your params'
            logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
            send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)

        # create a beutifulsoup from the first search results page
        first_soup = BeautifulSoup(search_result.text, 'html.parser')
        # find the Pager element, which contains the number of pages with search results, and convert it to a list
        pages = first_soup.find(attrs={'class': 'Pager'}).find_all('a')
        # convert page numbers to text
        pages = [p.text for p in pages if p.text != '']
        # start the downloaded results counter
        results_downloaded = 0

        # if there are any pages with the results
        if len(pages) > 0:
            # create a variable for counting the search results
            search_result_number = 1
            # loop through all search result page numbers
            for page in pages:
                # for each page number, create an url, params (see the 'part' parameter) and try to make get request,
                # including error handling
                url = 'https://www.rosnedra.gov.ru/index.fcgi'
                params={
                    'page': 'search',
                    'from_day': '28',
                    'from_month': '04',
                    'from_year': '2012',
                    'till_day': datetime.now().strftime('%d'),
                    'till_month': datetime.now().strftime('%m'),
                    'till_year': datetime.now().strftime('%Y'),
                    'q': search_string,
                    'step': '1',
                    'order': '2',
                    'part': page
                }
                try:
                    page_result = s.get(url, params=params, headers=headers, verify=False)
                    i = 1
                    while page_result.status_code != 200:
                        page_result = s.get(url, params=params, headers=headers, verify=False)
                        i += 1
                        if i > 100:
                            break
                except:
                    # send log message
                    message = f'Request to www.rosnedra.gov.ru search results page {url} failed, please check your params'
                    logf.write(f"{datetime.now().strftime(logdateformat)} Result #{search_result_number}. {message}\n")
                    send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)

                # create a beautifulsoup from the current search results page
                cur_search_results_page_soup = BeautifulSoup(page_result.text, 'html.parser')
                # find all search-result-item tags and put them to the list
                for search_result_item in cur_search_results_page_soup.find(attrs={'class': 'search-result-list'}).find_all(attrs={'class': 'search-result-item'}):
                    # set the locale to russian to be able to work with the item's date
                    locale.setlocale(locale.LC_ALL, locale='ru_RU.UTF-8')
                    # loop through search-result-link-info-item tags
                    for search_result_link_info_item in search_result_item.find_all(attrs={'class': 'search-result-link-info-item'}):
                        # if the search-result-link-info-item tag contains 'Дата' word, it's a datestamp
                        if 'Дата' in search_result_link_info_item.text:
                            # use a custom function to put the month name to nominative form
                            if platform.system() == 'Windows':
                                item_date = rus_month_genitive_to_nominative(search_result_link_info_item.text.lower())
                            else:
                                item_date = search_result_link_info_item.text.lower()
                            # extract the datestamp of the document
                            item_date = item_date.replace(u'\xa0', u' ')
                            item_date = item_date.replace('  ', ' ')
                            item_date = datetime.strptime(item_date.title(), 'Дата Документа: %d %B %Y')
                            pass
                            # item_date = datetime.strptime(item_date.title(), 'Дата Документа:\xa0\xa0%d\xa0%B\xa0%Y')

                    # check if the datestamp matches the given time period
                    if start <= item_date <= end:
                        # find the search-result-link tag inside the current search-result-item and extract url from it
                        url = 'https://' + f"rosnedra.gov.ru/{search_result_item.find(attrs={'class': 'search-result-link'})['href']}".replace('//', '/')
                        # make a standard process of requesting a page for the current search-result-item
                        try:
                            item_page_result = s.get(url)
                            i = 1
                            while item_page_result.status_code != 200:
                                page_result = s.get(url, verify=False)
                                i += 1
                                if i > 100:
                                    # send log message
                                    message = f"AuctionBlocksUpdater: Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Maximum tries to download {url} failed, please check your params"
                                    logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
                                    send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
                                    break
                        except:
                            # send log message
                            message = f"AuctionBlocksUpdater: Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Request to {url} failed, please check your params"
                            logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
                            send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
                        # create a beautifulsoup from search-result-item's webpage
                        cur_item_page_result_soup = BeautifulSoup(item_page_result.text, 'html.parser')
                        # find a content tag inside the page. It contents the links to the downloadable files.
                        cur_content_tags = cur_item_page_result_soup.find(attrs={'class': 'Content'})
                        # find all h1 tags to obtain the full document name and check if it contains the
                        # word 'Приказ Роснедр от'. This is a test to understand that we've found a Rosnedra
                        # order, not some other trash
                        cur_h1_tags = cur_item_page_result_soup.find_all('h1')
                        is_order = False
                        if cur_h1_tags:
                            for h1_tag in cur_h1_tags:
                                if 'Приказ Роснедр от' in h1_tag.text:
                                    is_order = True
                                    announcement = " ".join(h1_tag.text.replace('\xa0', ' ').split())
                                    ord_i = announcement.find('Приказ Роснедр от')
                                    order_date = datetime.strptime(announcement[ord_i:ord_i + 28], 'Приказ Роснедр от %d.%m.%Y')

                        # if we know that we've found an order and if it contains Content elements
                        if cur_content_tags and is_order:
                            # then we create a new folder to store the current results
                            final_directory = os.path.join(current_directory, folder)
                            final_directory = os.path.join(final_directory, f"{str(search_result_number)}_{item_date.strftime('%Y%m%d')}")
                            # if it already exists, delete it
                            if os.path.exists(final_directory):
                                shutil.rmtree(final_directory, ignore_errors=True)
                            # create a new folder
                            os.makedirs(final_directory)
                            # iterate the downloaded results counter
                            results_downloaded += 1
                            # create a new dictionary to store the order's metadata
                            metadata_dict = {}
                            # write order's url to metadata_dict
                            metadata_dict['url'] = url
                            # write order announce date to metadata_dict
                            metadata_dict['announce_date'] = item_date.strftime('%Y-%m-%d')
                            # extract the full name of a hyperlink and store it to the metadata_dict dictionary.
                            name = search_result_item.find(attrs={'class': 'search-result-link'}).text
                            metadata_dict['name'] = name
                            # store the order_date to metadata_dict
                            metadata_dict['order_date'] = order_date.strftime('%Y-%m-%d')

                            # find all </a> tags inside the Content element and loop through them
                            for item_doc_tag in cur_content_tags.find_all('a'):
                                # create a full url string from the current </a>
                                curl = f"https://www.rosnedra.gov.ru{item_doc_tag['href']}"
                                # extract a name of the document from the hyperlink text
                                cname = item_doc_tag.text
                                # standard process of downloading a file by the link and logging the errors.
                                try:
                                    dresult = s.get(curl)
                                    i = 1
                                    while dresult.status_code != 200:
                                        dresult = s.get(curl)
                                        i += 1
                                        if i > 100:
                                            # send log message
                                            message = f"AuctionBlocksUpdater: Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Maximum tries to download resource {curl} exceeded, please check your params"
                                            logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
                                            send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
                                            break
                                except:
                                    # send log message
                                    message = f"AuctionBlocksUpdater: Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Request to download resource {curl} from page {url} failed, please check your params"
                                    logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
                                    send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
                                # if something has been downloaded
                                if dresult.status_code == 200:
                                    # if we've downloaded at east 1 file, then function is a success
                                    success = True
                                    # create a new file using cname and the file extension from the curl
                                    with open(os.path.join(final_directory, f"{cname}.{curl.split('.')[-1]}"), 'wb') as f:
                                        f.write(dresult.content)
                                        pass
                            # find and parse the application deadline from the item webpage
                            for item_doc_p_tag in cur_content_tags.find_all('p'):
                                # if there are 'Последний срок приема' words in a <p> tag
                                # (which means that there is a deadline deascribed on a webpage):
                                if 'последнийсрокприема' in item_doc_p_tag.text.replace(' ', '').replace('\xa0', '').lower():
                                    deadlinestr = item_doc_p_tag.text
                                    deadlinestr = " ".join(deadlinestr.replace('\xa0', ' ').split())
                                    # if we are on Windows, replace month names from genitive to nominative
                                    if platform.system() == 'Windows':
                                        deadlinestr = rus_month_genitive_to_nominative(deadlinestr.lower())
                                    else:
                                        deadlinestr = deadlinestr.lower()
                                    # now we try to use several templates to parse the appliction deadline
                                    try:
                                        deadline = datetime.strptime(deadlinestr.title(), 'Последний Срок Приема Заявок: До %d %B %Y Г.')
                                    except:
                                        try:
                                            deadline = datetime.strptime(deadlinestr.title(), 'Последний Срок Приема Заявок: До %d %B %Y Г.)')
                                        except:
                                            try:
                                                deadline = datetime.strptime(deadlinestr.title(), 'Последний Срок Приема Заявок: До %H:%M (Местное Время) %d %B %Y Г.')
                                            except:
                                                try:
                                                    deadline = datetime.strptime(deadlinestr.title(), 'Последний Срок Приема Заявок: До %H:%M (Местное Время) %d %B %Y Г.)')
                                                except:
                                                    try:
                                                        deadline = datetime.strptime(deadlinestr.title(), 'Последний Срок Приема Заявок: До %H.%M (Местное Время) %d %B %Y Г.')
                                                        pass
                                                    except:
                                                        try:
                                                            deadline = datetime.strptime(deadlinestr.title(),'Последний Срок Приема Заявок: До %H.%M (Местное Время) %d %B %Y Г.)')
                                                        except:
                                                            # if none of our templates matched, then use 1970-01-01 as a fake deadline
                                                            deadline = datetime(1970, 1, 1)
                                                            # and send an error message to the log
                                                            message = f"AuctionBlocksUpdater: Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Could not parse application deadline from {url}, used the 1970-01-01. Please check the page content"
                                                            logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
                                                            send_to_telegram(s, logf, bot_info=bot_info,
                                                                             message=message,
                                                                             logdateformat=logdateformat)
                                    # now record the deadline to the metadata_dict. We don't use the time, just the date.
                                    # To use the time correctly we need to resolve the issue with timezones, because
                                    # Python uses local time zone by default.
                                    metadata_dict['deadline'] = deadline.strftime('%Y-%m-%d')
                            # now record all item's metadata to a json file in its folder
                            with open(os.path.join(final_directory, 'result_metadata.json'), "w", encoding='utf-8') as outfile:
                                json.dump(metadata_dict, outfile, ensure_ascii=False)
                        else:
                            # if the item is not an order, then send a message to log
                            message = f"AuctionBlocksUpdater: Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Attempt to parse items page {url} failed, please check the page content"
                            logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
                            send_to_telegram(s, logf, bot_info=bot_info, message=message,
                                             logdateformat=logdateformat)
                        # iterate the search result number
                        search_result_number += 1
        # return the locale settings to the initial state
        locale.setlocale(locale.LC_ALL, locale='')
        # write log message about results downloaded count
        message = f"AuctionBlocksUpdater: Rosnedra orders download from {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')} run successfully. " \
                  f"{results_downloaded} results downloaded."
        logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
        send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
        # and return a success result
        return success


def download_auc_results(start=datetime(year=2022, month=1, day=1), end=datetime.now(), search_string='Информация об итогах проведения аукциона на право пользования недрами', folder='rosnedra_auc_results', bot_info=('token', 'id')):
    success = False
    current_directory = os.getcwd()
    logdateformat = '%Y-%m-%d %H:%M:%S'
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    with open(log_file, 'a', encoding='utf-8') as logf, requests.Session() as s:
        message = 'AuctionResultsUpdater: Download data from Rosnedra started!'
        logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
        send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
        url = 'https://www.rosnedra.gov.ru/index.fcgi'
        params = {'page': 'search', 'step': '1', 'q': search_string}
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ru - RU, ru;q = 0.9, en - US;q = 0.8, en;q = 0.7, en - GB;q = 0.6',
            'Connection': 'keep-alive',
            'DNT': '1'
        }
        try:
            # make get request to the service
            search_result = s.get(url, params=params, headers=headers, verify=False)
            # i is a number of tries to retrieve a result
            i = 1
            # while the service returns anything other than 200 (OK):
            while search_result.status_code != 200:
                # make one more try
                search_result = s.get(url, params=params, headers=headers, verify=False)
                i += 1
                # until 100 attempts
                if i > 100:
                    break
        except:
            # if something went wrong, write a line to ligfile
            message = 'AuctionResultsUpdater: Initial request to www.rosnedra.gov.ru failed, please check your params'
            logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
            send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)

        # create a beutifulsoup from the first search results page
        first_soup = BeautifulSoup(search_result.text, 'html.parser')
        # find the Pager element, which contains the number of pages with search results, and convert it to a list
        pages = first_soup.find(attrs={'class': 'Pager'}).find_all('a')
        # convert page numbers to text
        pages = [p.text for p in pages if p.text != '']
        # start the downloaded results counter
        results_downloaded = 0

        # if there are any pages with the results
        if len(pages) > 0:
            # create a variable for counting the search results
            search_result_number = 1
            # loop through all search result page numbers
            for page in pages:
                # for each page number, create an url, params (see the 'part' parameter) and try to make get request,
                # including error handling
                url = 'https://www.rosnedra.gov.ru/index.fcgi'
                params = {
                    'page': 'search',
                    # 'from_day': '28',
                    'from_day': start.strftime('%d'),
                    # 'from_month': '04',
                    'from_month': start.strftime('%m'),
                    # 'from_year': '2012',
                    'from_year': start.strftime('%Y'),
                    # 'till_day': datetime.now().strftime('%d'),
                    # 'till_month': datetime.now().strftime('%m'),
                    # 'till_year': datetime.now().strftime('%Y'),
                    'till_day': end.strftime('%d'),
                    'till_month': end.strftime('%m'),
                    'till_year': end.strftime('%Y'),
                    'q': search_string,
                    'step': '1',
                    'order': '2',
                    'part': page
                }
                try:
                    page_result = s.get(url, params=params, headers=headers, verify=False)
                    i = 1
                    while page_result.status_code != 200:
                        page_result = s.get(url, params=params, headers=headers, verify=False)
                        i += 1
                        if i > 100:
                            break
                except:
                    # send log message
                    message = f'Request to www.rosnedra.gov.ru search results page {url} failed, please check your params'
                    logf.write(f"{datetime.now().strftime(logdateformat)} Result #{search_result_number}. {message}\n")
                    send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)

                # create a beautifulsoup from the current search results page
                cur_search_results_page_soup = BeautifulSoup(page_result.text, 'html.parser')
                # find all search-result-item tags and put them to the list
                for search_result_item in cur_search_results_page_soup.find(
                    attrs={'class': 'search-result-list'}).find_all(attrs={'class': 'search-result-item'}):
                    # set the locale to russian to be able to work with the item's date
                    locale.setlocale(locale.LC_ALL, locale='ru_RU.UTF-8')
                    # loop through search-result-link-info-item tags
                    for search_result_link_info_item in search_result_item.find_all(
                            attrs={'class': 'search-result-link-info-item'}):
                        # if the search-result-link-info-item tag contains 'Дата' word, it's a datestamp
                        if 'Дата' in search_result_link_info_item.text:
                            # use a custom function to put the month name to nominative form
                            if platform.system() == 'Windows':
                                item_date = rus_month_genitive_to_nominative(search_result_link_info_item.text.lower())
                            else:
                                item_date = search_result_link_info_item.text.lower()
                            # extract the datestamp of the document
                            item_date = item_date.replace(u'\xa0', u' ')
                            item_date = item_date.replace('  ', ' ')
                            item_date = datetime.strptime(item_date.title(), 'Дата Документа: %d %B %Y')
                    # check if the datestamp matches the given time period
                    if start <= item_date <= end:
                        # find the search-result-link tag inside the current search-result-item and extract url from it
                        url = 'https://' + f"rosnedra.gov.ru/{search_result_item.find(attrs={'class': 'search-result-link'})['href']}".replace(
                            '//', '/')
                        # make a standard process of requesting a page for the current search-result-item
                        try:
                            item_page_result = s.get(url)
                            i = 1
                            while item_page_result.status_code != 200:
                                page_result = s.get(url, verify=False)
                                i += 1
                                if i > 100:
                                    # send log message
                                    message = f"AuctionResultsUpdater: Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Maximum tries to download {url} failed, please check your params"
                                    logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
                                    send_to_telegram(s, logf, bot_info=bot_info, message=message,
                                                     logdateformat=logdateformat)
                                    break
                        except:
                            # send log message
                            message = f"AuctionResultsUpdater: Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Request to {url} failed, please check your params"
                            logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
                            send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
                        # create a beautifulsoup from search-result-item's webpage
                        cur_item_page_result_soup = BeautifulSoup(item_page_result.text, 'html.parser')
                        # find a content tag inside the page. It contents the links to the downloadable files.
                        cur_content_tags = cur_item_page_result_soup.find(attrs={'class': 'Content'})
                        # find all h1 tags to obtain the full document name and check if it contains the
                        # word 'Приказ Роснедр от'. This is a test to understand that we've found a Rosnedra
                        # order, not some other trash
                        cur_h1_tags = cur_item_page_result_soup.find_all('h1')
                        is_auc_result = False
                        if cur_h1_tags:
                            for h1_tag in cur_h1_tags:
                                if 'Информация об итогах проведения аукциона' in h1_tag.text:
                                    is_auc_result = True
                                    announcement = " ".join(h1_tag.text.replace('\xa0', ' ').split())
                        # if we know that we've found an auc result and if it contains Content elements
                        if cur_content_tags and is_auc_result:
                            # then we create a new folder to store the current results
                            final_directory = os.path.join(current_directory, folder)
                            final_directory = os.path.join(final_directory,
                                                           f"{str(search_result_number)}_{item_date.strftime('%Y%m%d')}")
                            # if it already exists, delete it
                            if os.path.exists(final_directory):
                                shutil.rmtree(final_directory, ignore_errors=True)
                            # create a new folder
                            os.makedirs(final_directory)
                            # iterate the downloaded results counter
                            results_downloaded += 1
                            # create a new dictionary to store the order's metadata
                            metadata_dict = {}
                            # write auc_result's url to metadata_dict
                            metadata_dict['url'] = url
                            # write order announce date to metadata_dict
                            metadata_dict['announce_date'] = item_date.strftime('%Y-%m-%d')
                            # extract the full name of a hyperlink and store it to the metadata_dict dictionary.
                            name = search_result_item.find(attrs={'class': 'search-result-link'}).text
                            metadata_dict['name'] = name

                            # find all <p> tags inside the Content element and loop through them
                            item_p_tags = cur_content_tags.find_all('p')
                            idx = item_p_tags[0].text.find('расположен')
                            lb_name = item_p_tags[0].text[:idx].lower().replace('в административном отношении', '').replace('участок', '').replace('недр', '').strip()
                            if 'на право пользования участком' in lb_name:
                                idx = lb_name.find('на право пользования участком')
                                lb_name = lb_name[idx + 30:]
                            if lb_name:
                                metadata_dict['license_block'] = lb_name.replace('«', '').replace('»', '').strip()
                            else:
                                metadata_dict['license_block'] = ''
                            metadata_dict['auction_held'] = 'NO'
                            content_list = []
                            for item_p_tag in list(item_p_tags):
                                content_list.append(item_p_tag.text)
                                if 'признан состоявшимся' in item_p_tag.text:
                                    metadata_dict['auction_held'] = 'YES'
                            metadata_dict['item_content_list'] = content_list
                            # now record all item's metadata to a json file in its folder
                            with open(os.path.join(final_directory, 'result_metadata.json'), "w",
                                      encoding='utf-8') as outfile:
                                json.dump(metadata_dict, outfile, ensure_ascii=False)
                                success = True
                        else:
                            # if the item is not an auc result, then send a message to log
                            message = f"AuctionResultsUpdater: Result #{search_result_number}_{item_date.strftime('%Y%m%d')}. Attempt to parse items page {url} failed, please check the page content"
                            logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
                            send_to_telegram(s, logf, bot_info=bot_info, message=message,
                                             logdateformat=logdateformat)
                            # iterate the search result number
                        search_result_number += 1
        # return the locale settings to the initial state
        locale.setlocale(locale.LC_ALL, locale='')
        # write log message about results downloaded count
        message = f"AuctionResultsUpdater: Rosnedra auction results download from {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')} run successfully. " \
                  f"{results_downloaded} results downloaded."
        logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
        send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
        # and return a success result
        return success


def parse_blocks_from_orders(folder='rosnedra_auc', gpkg='rosnedra_result.gpkg',
                             bot_info=('token', 'id'), report_bot_info=('token', 'id'),
                             blocks_np_webhook='', blocks_nr_ne_webhook=''):
    '''
    This function takes a folder with data downloaded from rosnedra.gov.ru by the download_orders function,
    parses license blocks coordinates and attributes from excel files and stores it into geopackage.
    ATTENTION this function will OVERWRITE the geopackage given in the gpkg parameter!
    This function requires datetime, osgeo.ogr, osgeo.osr, locale, os, fnmatch, shutil, platform, pandas as pd, json,
    vgdb_general.send_to_telegram.
    :param folder: name of the folder with the results of download_orders function at the same location with the script;
    :param gpkg: name of the geopackage to store the parsed results, OVERWRITING the file if it exists;
    :param bot_info: tuple containing two strings. This is the credentials to use to send log messages to a Telegram chat from a telegram bot. First string is a telegram token of a bot, second string is an id of a chat to send messages to. You can create a bot using @BotFather. To obtain chat id you need to send a message to the bot, then go to https://api.telegram.org/bot<Bot Token>/getUpdates page and look for something like "chat":{"id": 1234567 ...}. The id parameter is the chat id.
    :return: None
    '''
    # variable for function returna success
    success = False
    # get the current working directory
    current_directory = os.getcwd()
    # join the current working directory with the folder containing downloaded data
    directory = os.path.join(current_directory, folder)
    # define the datetime format for the logfile
    logdateformat = '%Y-%m-%d %H:%M:%S'
    # create a pathname for the logfile
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    # now we open the logfile and start logging
    with open(log_file, 'a', encoding='utf-8') as logf, requests.Session() as s:
        # send message to log
        message = 'AuctionBlocksUpdater: Rosnedra data parsing started!'
        logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
        send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
        # create a full path to the result geopackage
        gpkg_path = os.path.join(directory, gpkg)
        # create GSK-2011 CRS from proj string, using GOST 32453-2017 parameters
        gsk2011_crs = osr.SpatialReference()
        gsk2011_crs.ImportFromProj4('+proj=longlat +ellps=GSK2011 +towgs84=0.013,-0.092,-0.03,-0.001738,0.003559,-0.004263,0.00739999994614493 +no_defs +type=crs')
        # create WGS-84 CRS from proj string to avoid automatic datum transformation
        wgs84_crs = osr.SpatialReference()
        wgs84_crs.ImportFromProj4('+proj=longlat +datum=WGS84 +no_defs')
        # create a transformation from GSK-2011 to WGS-84. We will assume that all coordinates are given in GSK-2011.
        transform_gsk_to_wgs = osr.CoordinateTransformation(gsk2011_crs, wgs84_crs)
        # create a peopackage driver
        gdriver = ogr.GetDriverByName('GPKG')
        # if geopackage already exists, delete it.
        if os.path.exists(gpkg_path):
            gdriver.DeleteDataSource(gpkg_path)
        # create a ogr datasource for the result geopackage
        gdatasource = gdriver.CreateDataSource(gpkg_path)
        # create a layer for the parsed license blocks
        out_layer = gdatasource.CreateLayer('license_blocks_rosnedra_orders', srs=wgs84_crs, geom_type=ogr.wkbPolygon)
        # out_layer = gdatasource.CreateLayer('license_blocks_rosnedra_orders', srs=wgs84_crs, geom_type=ogr.wkbMultiPolygon)
        # create a list of fieldnames for license blocks
        field_names = ['resource_type', 'name', 'area_km', 'reserves_predicted_resources', 'exp_protocol', 'usage_type', 'lend_type', 'planned_terms_conditions', 'source_name', 'source_url', 'order_date', 'announce_date', 'appl_deadline']
        # create a list of field types for license blocks. The order must match the field_names list.
        field_types = [ogr.OFTString, ogr.OFTString, ogr.OFTReal, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTString, ogr.OFTDate, ogr.OFTDate, ogr.OFTDate]
        # add fields to the result layer
        for f_name, f_type in zip(field_names, field_types):
            out_layer.CreateField(ogr.FieldDefn(f_name, f_type))
        # get the layer definition from the layer to use it when creating new features
        featureDefn = out_layer.GetLayerDefn()
        # start the parsed blocks counter
        blocks_parsed = 0

        # create empty list for new blocks for telegram report
        new_blocks_list = []

        # loop through the folders inside the folder with downloaded data
        for path, dirs, files in os.walk(os.path.abspath(directory)):
            # loop through all excel files in the current folder. Usually it is 1 excel file.
            for filename in fnmatch.filter(files, '*.xls*'):
                # load current item metadata from result_metadata.json file in the current folder. The file is created by
                # download_orders function.
                with open(os.path.join(path, 'result_metadata.json'), 'r', encoding='utf-8') as jf:
                    meta_dict = json.load(jf)
                # create a fullpath to the current excel file
                excel_file = os.path.join(path, filename)
                # use pandas to read the excel file. Python environment must have both openpyxl and xlrd libraries installed
                # to support both *.xls and *.xlsx
                df = pd.read_excel(excel_file)
                # get the number of rows and columns in the current excel file
                nrows, ncols = df.shape
                # create initial values for block id and polygon ring id
                block_id = 0
                ring_id = 0
                # create the first current polygon ring object
                cur_ring = ogr.Geometry(ogr.wkbLinearRing)
                # create the first current license block geometry object
                cur_block_geom = ogr.Geometry(ogr.wkbPolygon)
                # Create a list with excel column numbers for the attribute fields from field_names list
                field_cols = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                # create dictionary to store the column names and their column numbers in excel file.
                # We will initially make all column numbers to 0, but later will update them to real numbers.
                # Initially we put these column names to the dict, which are the number of a block inside current excel,
                # number of a point in a ring inside block, and DD, MM, SS.SSSSS values for Lat and Lon
                excel_col_nums = {'block_num': 0, 'point_num': 0, 'y_d': 0, 'y_m': 0, 'y_s': 0, 'x_d': 0, 'x_m': 0, 'x_s': 0}
                # next, we add the layer field names to the dict, also with 0 excel column numbers initially.
                excel_col_nums.update(dict(zip(field_names, field_cols)))

                # this was an attempt to store the local time of the order using correct timezone (failed)
                # tf = TimezoneFinder()

                # loop through the rows in the current excel spreadsheet
                for nrow in range(nrows):
                    # loop through the columns in the current row
                    for ncol in range(ncols):
                        # now we start to update the column numbers in the excel_col_nums dictionary.
                        # first, we look for the keyword 'град' inside a cell, presuming that all excel files have
                        # a header for coordinate columns with this word. We also presume that the coordinates
                        # are given either in 6 columns GSK-2011 only or in 6 columns GSK-2011 first and in other CRS
                        # in next 6 columns.
                        # So if we've found 'град' keyword in a cell, we decide that we are on the first column containing
                        # coordinates which is degrees latitude in GSK-2011.
                        if 'град' in str(df.iloc[nrow, ncol]).replace(' ', '').replace('\n', '') \
                                and 'мин' in str(df.iloc[nrow, ncol + 1]).replace(' ', '').replace('\n', '') \
                                and excel_col_nums['y_d'] == 0:
                            # start populating excel column numbers in our dictionary
                            excel_col_nums['y_d'] = ncol
                            excel_col_nums['y_m'] = ncol + 1
                            excel_col_nums['y_s'] = ncol + 2
                            excel_col_nums['x_d'] = ncol + 3
                            excel_col_nums['x_m'] = ncol + 4
                            excel_col_nums['x_s'] = ncol + 5
                            # we presume that a column before coordinates contains point number
                            excel_col_nums['point_num'] = ncol - 1
                            # and we also presume that previous column contains license block area
                            excel_col_nums['area_km'] = ncol - 2

                            excel_col_nums['block_num'] = ncol - 5

                        # now we start checking if the cell contains a fieldname keywords. If we find a keyword in a cell,
                        # then we use its column number to put into excel_col_nums dictionary
                        if str(df.iloc[nrow, ncol]).replace(' ', '').replace('\n', '').lower() == 'видполезногоископаемого' and excel_col_nums['resource_type'] == 0:
                            excel_col_nums['resource_type'] = ncol
                        if 'наименованиеучастканедр' in str(df.iloc[nrow, ncol]).replace('\n', '').replace(' ', '').lower() and excel_col_nums['name'] == 0:
                            excel_col_nums['name'] = ncol
                        if 'ресурсы' in str(df.iloc[nrow, ncol]).replace('\n', '').lower() and excel_col_nums['reserves_predicted_resources'] == 0:
                            excel_col_nums['reserves_predicted_resources'] = ncol
                        if 'протокол' in str(df.iloc[nrow, ncol]).replace('\n','').lower() and excel_col_nums['exp_protocol'] == 0:
                            excel_col_nums['exp_protocol'] = ncol
                        if 'видпользованиянедрами' in str(df.iloc[nrow, ncol]).replace('\n','').replace(' ','').lower() and excel_col_nums['usage_type'] == 0:
                            excel_col_nums['usage_type'] = ncol
                        if 'формапредоставленияучастканедрвпольз' in str(df.iloc[nrow, ncol]).replace('\n','').replace(' ','').lower()  and excel_col_nums['lend_type'] == 0:
                            excel_col_nums['lend_type'] = ncol
                        if 'планируемыесрокипроведения' in str(df.iloc[nrow, ncol]).replace('\n', '').replace(' ', '').lower() and excel_col_nums['planned_terms_conditions'] == 0:
                            excel_col_nums['planned_terms_conditions'] = ncol

                    # as a result, after looping through first several rows of the excel, we will have
                    # excel_col_nums dictionary populated with excel column numbers for each field, or zeros
                    # if a field keywords haven't been found

                    # at each row, we populate point_n and y_d variables with values in according with excel_col_nums
                    point_n = df.iloc[nrow, excel_col_nums['point_num']]
                    y_d = df.iloc[nrow, excel_col_nums['y_d']]
                    # if we have a point with number 1 OR if we have text in point_n column,
                    # then we are at a new ring's first point row.
                    if str(point_n) == '1' or (cur_ring.GetPointCount() > 2 and not str(y_d).isdigit()):
                        # if we're not on the first ring,
                        if ring_id > 0:
                            # and if we have at least 3 points in a current ring,
                            if cur_ring.GetPointCount() > 2:
                                # then we close the current ring
                                cur_ring.CloseRings()
                                # and add it to current block's geometry
                                cur_block_geom.AddGeometry(cur_ring)
                        # iterate the ring id inside the current excel
                        ring_id += 1
                        # and create next current ring
                        cur_ring = ogr.Geometry(ogr.wkbLinearRing)
                    # now we check if we are at a block's first row.
                    # We check that a block number column has some value, and that the row has digital coordinates

                    ##########ЗДЕСЬ СУКА ЖУЧАРА СИДИТ, В ЭТОТ IF НЕ ПОПАДАЕМ НИ РАЗУ
                    if str(df.iloc[nrow, excel_col_nums['block_num']]) != 'nan' and len(str(df.iloc[nrow, excel_col_nums['block_num']])) > 0 and str(df.iloc[nrow, excel_col_nums['y_s']]).replace(',', '').replace('.', '').isdigit() and str(df.iloc[nrow, excel_col_nums['y_s']]) != 'nan':
                    ##########

                        # if this is not the first block,
                        if block_id > 0:
                            # if we have at least 3 points in a current ring,
                            if cur_ring.GetPointCount() > 2:
                                # then we close the current ring,
                                cur_ring.CloseRings()
                                # add the current ring to the current block geometry,
                                cur_block_geom.AddGeometry(cur_ring)
                            # then we close all rings in current block,
                            cur_block_geom.CloseRings()
                            # make coordinate transformation from GSK-2011 to WGS-84,
                            cur_block_geom.Transform(transform_gsk_to_wgs)
                            # create a new feature for the layer
                            feature = ogr.Feature(featureDefn)
                            # and set the feature's geometry to the current block's geometry.
                            feature.SetGeometry(cur_block_geom)
                            # add an item to the list of new blocks for telegram report
                            new_blocks_list.append(attrs_dict)
                            # next we populate the feature's attributes with current block attribute values
                            for f_name, f_val in zip(field_names, field_vals):
                                if f_name == 'appl_deadline' and f_val:
                                    # # this was a failed attempt to store the local order time within deadline field
                                    # tz = tf.timezone_at(lng=cur_block_geom.Centroid().GetX(),
                                    #                     lat=cur_block_geom.Centroid().GetY())
                                    # f_val_dt = datetime.strptime(f_val, '%Y-%m-%d %H:%M')
                                    # f_val_dt = f_val_dt.replace(tzinfo=ZoneInfo(tz))
                                    # tzinfo = ZoneInfo(tz)
                                    # os.environ['TZ'] = tz
                                    # time.tzset()
                                    # windows_timezone = f"{tz[tz.find('/') + 1:]} Standard Time"
                                    # os.environ['TZ'] = windows_timezone
                                    # os.system(f"tzutil /s \"{windows_timezone}\"")
                                    feature.SetField(f_name, f_val)
                                else:
                                    feature.SetField(f_name, f_val)
                            # iterate the parsed blocks counter
                            blocks_parsed += 1
                            # and add a new feature to the layer.
                            out_layer.CreateFeature(feature)
                            # and function is a success if we've stored at least 1 feature
                            success = True
                        # now we iterate the block_id inside the current excel
                        block_id += 1

                        # # This is again more failed attempts to parse the local deadline time
                        # if meta_dict.get('deadline') != None:
                        #     deadlinedt = datetime.strptime(meta_dict.get('deadline'), '%Y-%m-%d %H:%M')
                        #     pass
                        # else:
                        #     deadlinedt = meta_dict.get('deadline')
                        #     pass

                        # assuming that we are at the new block's first row, we store the block's attribute values
                        # to our field_vals list in the correct order
                        field_vals = [
                            df.iloc[nrow, excel_col_nums['resource_type']],
                            df.iloc[nrow, excel_col_nums['name']],
                            float(str(df.iloc[nrow, excel_col_nums['area_km']]).replace(',', '.')),
                            df.iloc[nrow, excel_col_nums['reserves_predicted_resources']],
                            df.iloc[nrow, excel_col_nums['exp_protocol']],
                            df.iloc[nrow, excel_col_nums['usage_type']],
                            df.iloc[nrow, excel_col_nums['lend_type']],
                            df.iloc[nrow, excel_col_nums['planned_terms_conditions']],
                            meta_dict['name'],
                            meta_dict['url'],
                            # datetime.strptime(path[-8:], '%Y%m%d').strftime('%Y-%m-%d')
                            meta_dict['order_date'],
                            meta_dict['announce_date'],
                            # as not all orders contain deadlines, so we use get method to receive None if there is no one.
                            meta_dict.get('deadline')
                        ]
                        # create a dict with attributes for the telegram report
                        attrs_dict = dict(zip(field_names, field_vals))
                        # create a new current block geometry
                        cur_block_geom = ogr.Geometry(ogr.wkbPolygon)
                        # and set the ring_id value to 1.
                        ring_id = 1

                    # now we check that we have coordinates in the current row, read them, convert and add to the current ring
                    y_s = df.iloc[nrow, excel_col_nums['y_s']]
                    if str(y_s).replace(',', '').replace('.', '').isdigit() and str(df.iloc[nrow, excel_col_nums['y_s']]) != 'nan':
                        y = float(str(df.iloc[nrow, excel_col_nums['y_d']]).replace(',', '.')) + \
                            float(str(df.iloc[nrow, excel_col_nums['y_m']]).replace(',', '.')) / 60 + \
                            float(str(df.iloc[nrow, excel_col_nums['y_s']]).replace(',', '.')) / 3600
                        x = float(str(df.iloc[nrow, excel_col_nums['x_d']]).replace(',', '.')) + \
                            float(str(df.iloc[nrow, excel_col_nums['x_m']]).replace(',', '.')) / 60 + \
                            float(str(df.iloc[nrow, excel_col_nums['x_s']]).replace(',', '.')) / 3600
                        cur_ring.AddPoint(x, y)

                # now we add the last block read from file to the layer.
                # if we've read at least one block,
                if block_id > 0:
                    # and if the current ring has at least 3 points,
                    if cur_ring.GetPointCount() > 2:
                        # then close the current ring
                        cur_ring.CloseRings()
                        # and add it to the current block's geometry.
                        cur_block_geom.AddGeometry(cur_ring)
                    # Then close all the rings in the current block,
                    cur_block_geom.CloseRings()
                    # transform the coordinates from GSK-2011 to WGS-84,
                    cur_block_geom.Transform(transform_gsk_to_wgs)
                    # create a new feature
                    feature = ogr.Feature(featureDefn)
                    # and set its geometry to the current block's geometry.
                    feature.SetGeometry(cur_block_geom)
                    # add an item to the list of new blocks for telegram report
                    new_blocks_list.append(attrs_dict)

                    # next we populate the feature's attributes with current block attribute values
                    for f_name, f_val in zip(field_names, field_vals):
                        if f_name == 'appl_deadline' and f_val:
                        # # this was a failed attempt to store the local order time within deadline field
                        #     tz = tf.timezone_at(lng=cur_block_geom.Centroid().GetX(), lat=cur_block_geom.Centroid().GetY())
                        #     f_val_dt = datetime.strptime(f_val, '%Y-%m-%d %H:%M')
                        #     f_val_dt = f_val_dt.replace(tzinfo=ZoneInfo(tz))
                        #     tzinfo = ZoneInfo(tz)
                        #     # os.environ['TZ'] = tz
                        #     # time.tzset()
                        #     windows_timezone = f"{tz[tz.find('/') + 1:]} Standard Time"
                        #     os.environ['TZ'] = windows_timezone
                        #     os.system(f"tzutil /s \"{windows_timezone}\"")
                        #     print('hi')
                            feature.SetField(f_name, f_val)
                            pass
                        feature.SetField(f_name, f_val)
                    # iterate the parsed blocks counter
                    blocks_parsed += 1
                    # and add a new feature to the layer.
                    out_layer.CreateFeature(feature)
                    # and set function result to success if we've added at least 1 feature

        # If new blocks contain any HCS blocks, send report to telegram            success = True
        new_hcs_blocks_list = [x for x in new_blocks_list if any(['нефт' in str(x['resource_type']), 'газ' in str(x['resource_type']), 'конденсат' in str(x['resource_type'])])]
        new_np_blocks_list = [x for x in new_hcs_blocks_list if
                              'для геологического изучения недр' in str(x['source_name']).lower()]
        new_nr_ne_blocks_list = [x for x in new_hcs_blocks_list if
                              'для разведки и добычи полезных ископаемых' in str(x['source_name']).lower()]

        if blocks_np_webhook:
            for new_np_block in new_np_blocks_list:
                np_block_name = ' '.join(new_np_block['name'].replace('\n', ' ').split())
                message = f"Новый участок УВС НП в приказе Роснедра:\n{str(new_np_block['resource_type'])}; " \
                          f"\nПриказ от {new_np_block['order_date']}" \
                          f"\n{np_block_name}" \
                          f"\nСрок подачи заявки: {(new_np_block['appl_deadline'] or 'Неизвестен')}"
                send_to_teams(blocks_np_webhook, message, logf, button_text='Открыть объявление', button_link=new_np_block['source_url'])

        if blocks_nr_ne_webhook:
            for new_nr_ne_block in new_nr_ne_blocks_list:
                nr_ne_block_name = ' '.join(new_nr_ne_block['name'].replace('\n', ' ').split())
                message = f"Новый участок УВС НР, НЭ в приказе Роснедра:\n{str(new_nr_ne_block['resource_type'])}; " \
                          f"\nПриказ от {new_nr_ne_block['order_date']}" \
                          f"\n{nr_ne_block_name}" \
                          f"\nСрок подачи заявки: {(new_nr_ne_block['appl_deadline'] or 'Неизвестен')}"
                send_to_teams(blocks_nr_ne_webhook, message, logf, button_text='Открыть объявление', button_link=new_nr_ne_block['source_url'])

        if new_hcs_blocks_list:
            message = f"Загружено {str(len(new_hcs_blocks_list))} новых объявлений о выставлении участков УВС на аукционы:\n"
            for j, hcs_block in enumerate(new_hcs_blocks_list):
                hcs_block_name = ' '.join(hcs_block['name'].replace('\n', ' ').split())
                message += '\n' + f"({str(j + 1)}) {str(hcs_block['resource_type'])}; Приказ от {hcs_block['order_date']}; {hcs_block_name}; Срок подачи заявки: {(hcs_block['appl_deadline'] or 'Неизвестен')}; {hcs_block['source_url']}" + '\n'
            # message += '\n'.join([str(x['resource_type']) + '; Приказ от ' + x['order_date'] + '; ' + x['name'].replace('\n', ' ') + '; Срок подачи заявки: ' + (x['appl_deadline'] or 'Неизвестен') + '; ' for x in new_hcs_blocks_list])
            send_to_telegram(s, logf, bot_info=report_bot_info, message=message, logdateformat=logdateformat)

        # finally, send a message to the log describing how many block have we totally parsed
        message = f"AuctionBlocksUpdater: downloaded Rosnedra orders data parsed successfully. {blocks_parsed} blocks parsed."
        logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
        send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
        return success


def get_latest_order_date_from_synology(pgconn):
    '''
    This function returns the latest Rosnedra order announce date recorded to the \n
    rosnedra.license_blocks_rosnedra_orders table inside the specified database.
    :param pgconn: psycopg2 connection object to the postgres database
    :return: tuple of 2 elements: (bool_success, datetime_object)
    '''
    try:
        with pgconn.cursor() as cur:
            cur.execute("SELECT max(announce_date) as latest_announce_date FROM rosnedra.license_blocks_rosnedra_orders")
            ldate = cur.fetchall()[0][0]
            ldatetime = datetime(ldate.year, ldate.month, ldate.day)
            return (True, ldatetime)
    except:
        return (False, None)


def get_latest_auc_result_date_from_synology(pgconn):
    '''
    This function returns the latest Rosnedra order announce date recorded to the \n
    rosnedra.license_blocks_rosnedra_orders table inside the specified database.
    :param pgconn: psycopg2 connection object to the postgres database
    :return: tuple of 2 elements: (bool_success, datetime_object)
    '''
    try:
        with pgconn.cursor() as cur:
            cur.execute("SELECT max(announce_date) as latest_announce_date FROM rosnedra.auc_results")
            ldate = cur.fetchall()[0][0]
            ldatetime = datetime(ldate.year, ldate.month, ldate.day)
            return (True, ldatetime)
    except:
        return (False, None)


def update_postgres_table(gdalpgcs, folder='rosnedra_auc', gpkg='rosnedra_result.gpkg', table='rosnedra.license_blocks_rosnedra_orders', bot_info=('token', 'chatid')):
    '''
    This function takes the results of the parse_blocks_from_orders function and dumps them to the \n
    rosnedra.license_blocks_rosnedra_orders table inside the specified database. \n
    This function requires datetime, requests, osgeo.ogr, osgeo.osr, osgeo.gdal. \n
    :param gdalpgcs: string. Gdal connection string for Postgres database in the following format PG:host=<host> dbname=<database> user=<user> password=<password> port=<port>\n
    :param folder: string. A folder name containing parse_blocks_from_orders results.
    :param gpkg: string. Geopackage name inside the <folder> containing the license_blocks_rosnedra_orders layer.
    :param bot_info: tuple containing two strings. This is the credentials to use to send log messages to a Telegram chat from a telegram bot. First string is a telegram token of a bot, second string is an id of a chat to send messages to. You can create a bot using @BotFather. To obtain chat id you need to send a message to the bot, then go to https://api.telegram.org/bot<Bot Token>/getUpdates page and look for something like "chat":{"id": 1234567 ...}. The id parameter is the chat id.
    :return: None
    '''
    success = False
    # get the current working directory
    current_directory = os.getcwd()
    # get the path to the folder
    directory = os.path.join(current_directory, folder)
    # define the datetime format for the logfile
    logdateformat = '%Y-%m-%d %H:%M:%S'
    # create a pathname for the logfile
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    # open the log file and start a requests session to send messages to Telegram
    with open(log_file, 'a', encoding='utf-8') as logf, requests.Session() as s:
        # send a log message about the start of table update
        message = 'AuctionBlocksUpdater: Synology table rosnedra.license_blocks_rosnedra_orders update started!'
        logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
        send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)

        # create a standard EPSG WGS-1984 CRS
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        # create a path to the geopackage
        sourcepath = os.path.join(folder, gpkg)
        # create an ogr datasource
        sourceds = gdal.OpenEx(sourcepath, gdal.OF_VECTOR)

        # create VectorTranslateOptions to specify the data conversion parameters
        # layerName: full destination table name
        # format: destination format
        # accessMode: append or overwrite data to the destination
        # dstSRS: destination CRS
        # layers: source layer names list
        # geometryType: destination geometry type
        myoptions = gdal.VectorTranslateOptions(
            layerName=table,
            format='PostgreSQL',
            accessMode='append',
            dstSRS=srs,
            layers=['license_blocks_rosnedra_orders'],
            geometryType='MULTIPOLYGON'
        )
        try:
            i = 1
            while i < 10:
                # try to do the conversion
                if gdal.VectorTranslate(gdalpgcs, sourceds, options=myoptions):
                    success = True
                    # if OK, then send the log message
                    message = f"AuctionBlocksUpdater: Synology table rosnedra.license_blocks_rosnedra_orders updated successfully."
                    logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
                    send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
                    break
                else:
                    i += 1
                    message = f"AuctionBlocksUpdater: Synology table rosnedra.license_blocks_rosnedra_orders updated FAILED, retrying (attempt {i} of 10)..."
                    logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
                    send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
            if i > 10:
                message = "Synology table rosnedra.license_blocks_rosnedra_orders update FAILED after 10 attempts."
                logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
                send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
        except:
            # if not OK, then send the log message
            message = "Synology table rosnedra.license_blocks_rosnedra_orders update FAILED."
            logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
            send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
    return success


def update_postgres_auc_results_table(pgconn, folder='rosnedra_auc_results', table='rosnedra.auc_results', bot_info=('token', 'chatid')):
    success = False
    # get the current working directory
    current_directory = os.getcwd()
    # get the path to the folder
    directory = os.path.join(current_directory, folder)
    # define the datetime format for the logfile
    logdateformat = '%Y-%m-%d %H:%M:%S'
    # create a pathname for the logfile
    log_file = os.path.join(current_directory, folder, 'logfile.txt')
    # open the log file and start a requests session to send messages to Telegram
    results_list = []
    with open(log_file, 'a', encoding='utf-8') as logf, requests.Session() as s:
        # send a log message about the start of table update
        message = 'AuctionResultsUpdater: Synology table rosnedra.auc_results update started!'
        logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
        send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
        for path, dirs, files in os.walk(os.path.abspath(directory)):
            for filename in fnmatch.filter(files, 'result_metadata.json'):
                with open(os.path.join(path, 'result_metadata.json'), 'r', encoding='utf-8') as jf:
                    meta_dict = json.load(jf)
                    results_list.append(meta_dict)
        values_to_insert_lists = []
        for result in results_list:
            values_to_insert_lists.append([f"'{result['url']}'"
                                             , f"'{result['announce_date']}'"
                                             , "'" + result['name'].replace("'", "''") + "'"
                                             , f"'{result['license_block']}'"
                                             , f"'{result['auction_held']}'"
                                             , '\n'.join(["'" + x.replace("'", "''") + "'" for x in result['item_content_list']])])
        sql = f"insert into {table}(url, announce_date, title, license_block, auction_success, content)" \
              f" values{', '.join(['(' + ', '.join(x) + ')' for x in values_to_insert_lists])};"
        with pgconn.cursor() as cur:
            try:
                cur.execute(sql)
                pgconn.commit()
                message = f"AuctionBlocksUpdater: Synology table rosnedra.auc_results updated successfully."
                logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
                send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)
            except:
                message = f"AuctionBlocksUpdater: Synology table rosnedra.auc_results failed"
                logf.write(f"{datetime.now().strftime(logdateformat)} {message}\n")
                send_to_telegram(s, logf, bot_info=bot_info, message=message, logdateformat=logdateformat)


def clear_folder(folder):
    for root, dirs, files in os.walk(folder):
        for f in files:
            if f != 'logfile.txt':
                os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))


# with open('.pgdsn', encoding='utf-8') as dsnf:
#     dsn = dsnf.read().replace('\n', '')
#
# with psycopg2.connect(dsn) as pgconn:
#     startdt = get_latest_order_date_from_synology(pgconn) + timedelta(days=1)
#
# with open('.pggdal', encoding='utf-8') as gdalf:
#     gdalpgcs = gdalf.read().replace('\n', '')
#
# # This is telegram credentials to send message to stepanosokin
# with open('bot_info_vgdb_bot_toStepan.json', 'r', encoding='utf-8') as f:
#     jdata = json.load(f)
#     bot_info = (jdata['token'], jdata['chatid'])
#
# # This is telegram credentials to send message to the 'VG Database Techinfo' group
# # with open('bot_info_vgdb_bot_toGroup.json', 'r', encoding='utf-8') as f:
# #     jdata = json.load(f)
# #     bot_info = (jdata['token'], jdata['chatid'])
# #
# clear_folder('rosnedra_auc')
# #
# download_orders(start=datetime(2023, 5, 13), end=datetime.now(), search_string='Об утверждении Перечня участков недр', folder='rosnedra_auc', bot_info=bot_info)
# #
# parse_blocks_from_orders(folder='rosnedra_auc', gpkg='rosnedra_result.gpkg', bot_info=bot_info)
#
# update_synology_table(gdalpgcs, folder='rosnedra_auc', bot_info=bot_info)
