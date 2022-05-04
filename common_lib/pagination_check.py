from __future__ import annotations
from errno import EKEYEXPIRED
import os
import sys
import logging
import re
import urllib
from logging import Logger
from typing import Any, Union
import pickle
from bs4 import BeautifulSoup as bs4
from bs4.element import Tag
from bs4.element import ResultSet
from datetime import datetime
from dateutil.parser import parse
from prefect_lib.settings import TIMEZONE
import time
import requests


def patination_check(url:str, request_text:str):
    ''' '''

    _ = re.compile(r'.html$')
    temp1 = _.sub('',url)
    print('temp1: ',temp1)

    _ = re.compile(r'\d+$')
    url_pattern = _.sub('',temp1)
    print('url_pattern: ',url_pattern)

    soup: bs4 = bs4(request_text, 'lxml')
    css_selecter = r'[href]'
    #css_selecter = fr'[href=articleBody]'
    print('css_selecter: ',css_selecter)
    scraped_item = soup.select(css_selecter)
    for a in scraped_item:
        urllib.parse.urljoin(base_url, rel_url)
        print(a.attrs['href'])


    return '' #scraped_result, scraped_pattern


if __name__ == '__main__':
    '''単体テスト用の設定'''
    test_url = 'https://www.sankei.com/politics/news/210521/plt2105210030-n1.html'
    #test_url = 'https://www.sankei.com/article/20220402-4DJ4CTA7MBKVHIH7KX3CA2FYGY/'
    # 通常サイト用
    #request = requests.get(test_url)
    # JavaScript動作後の取得用。Splashを利用。
    request = requests.get('http://localhost:8050/render.html',
                          params={'url': test_url, 'wait': 0.5})

    result = patination_check(
        url=test_url,
        request_text=request.text,
    )
    #print('\n\n=== result ===', result)
