import json
import os
import random
import re
import time
from typing import Tuple, List

import requests
from bs4 import BeautifulSoup


# GET all sql examples from w3schools (YEAR 2023)
ROOT_URL = 'https://www.w3schools.com/sql/'
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.50',
    'Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1041.0 Safari/535.21',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36',
    'Opera/9.80 (Android 2.3.4; Linux; Opera Mobi/build-1107180945; U; en-GB) Presto/2.8.149 Version/11.10',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)'
]
REQUEST_INTERVAL_MIN = 5
REQUEST_INTERVAL_MAX = 10
OUTPUT_FILE = 'sqls.json'


def _page_url(page):
    return ROOT_URL + page


def _root_page():
    return _page_url('default.asp')


def _headers(referer_page=''):
    if not referer_page:
        referer = _root_page()
    else:
        referer = _page_url(referer_page)
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Referer': referer
    }


def _dump_page(page):
    url = _page_url(page)
    resp = requests.get(url)
    print(resp.text)


def _get_all_pages() -> List[Tuple[str, str]]:
    # get root page
    page_url = ROOT_URL
    print('get all pages from %s' % page_url)
    resp = requests.get(page_url, headers=_headers())
    soup = BeautifulSoup(resp.text, 'html.parser')

    # find leftmenuinnerinner
    left_menu = soup.find(id='leftmenuinnerinner')

    # find all pages at <a target="_top">
    links = left_menu.find_all(name='a', target='_top')
    pages = []
    for link in links:
        pages.append((link.text, link['href']))
    print('all pages are: %s' % json.dumps(pages, indent=2, ensure_ascii=False))
    print('overall %d pages!' % len(pages))

    return pages


def _load_saved_data():
    try:# returns: json loaded data, saved categories
        categories = set()
        if not os.path.exists(OUTPUT_FILE):
            return [], categories
        sqls = json.loads(open(OUTPUT_FILE, encoding='utf8').read())
        for sql_data in sqls:
            category = sql_data.get('category')
            if category:
                categories.add(category)
        return sqls, categories
    except Exception as e:
        print('load saved data err: %s' % e)
        return [], set()


def _crawl_sqls(name, page, referer_page=''):
    sql_set = set()
    url = _page_url(page)
    print('[%s] crawling page url %s...' % (name, url))
    resp = requests.get(url, headers=_headers(referer_page=referer_page))
    soup = BeautifulSoup(resp.text, 'html.parser')

    # get all examples in class="w3-code notranslate sqlHigh"
    sql_blocks = soup.find_all(class_='w3-code notranslate sqlHigh')
    print('[%s] overall %d sql blocks' % (name, len(sql_blocks)))
    for sql_block in sql_blocks:
        # some children blocks may not contain space, which leads to
        # extracting non-separated SQLs like -> SELECT * FROM CustomersLIMIT 3;
        # so we should use get_text API
        # instead of using sql_block.text straightly
        sql_text_no_endl = sql_block.get_text(' ', strip=True).replace('\n', ' ').strip()
        sql_text = re.sub(r'\s+', ' ', sql_text_no_endl)
        if not sql_text.endswith(';'):  # add semicolon
            sql_text = sql_text + ';'

        if sql_text not in sql_set:
            print('[%s] crawled new sql -> %s' % (name, sql_text))
            sql_set.add(sql_text)
        else:
            print('[%s] detected duplicated sql -> %s' % (name, sql_text))

    sqls = list(sql_set)
    print('[%s] crawl finished! added %d sqls!' % (name, len(sqls)))
    return sqls


def crawl_all_sqls():
    pages = _get_all_pages()

    sqls_all, saved_categories = _load_saved_data()
    print('load saved categories -> %s' % list(saved_categories))

    prev_page = ''
    page_cnt = 0
    for name, page in pages:
        page_cnt += 1

        # skip crawled pages
        if name in saved_categories:
            print('detected category %s already crawled, skip...' % name)
            continue

        # sleep for couple of seconds
        request_interval = random.randint(REQUEST_INTERVAL_MIN, REQUEST_INTERVAL_MAX)
        print('sleep for %d secs...' % request_interval)
        time.sleep(request_interval)

        # crawl sql
        sqls = _crawl_sqls(name, page, referer_page=prev_page)

        # extend sqls, we allow same SQL in different categories
        sqls_all.extend(list(map(lambda s: {
            'category': name,
            'sql': s
        }, sqls)))

        # save to output file
        sqls_str = json.dumps(sqls_all, indent=2, ensure_ascii=False)
        print('currently %d sqls, saving data...' % len(sqls_all))
        with open(OUTPUT_FILE, encoding='utf-8', mode='w') as f:
            f.write(sqls_str)
            f.close()

        # update prev page
        prev_page = page
        print('currently crawled %d/%d pages...' % (page_cnt, len(pages)))

    # dump to file
    print('finished crawling all sqls!')


if __name__ == '__main__':
    # _dump_page('sql_select.asp')
    # _crawl_sqls('SELECT', 'sql_select.asp', set())
    crawl_all_sqls()
