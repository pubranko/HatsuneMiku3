from errno import EKEYEXPIRED
import os
import sys
import logging
from logging import Logger
from typing import Any, Union
import pickle
from bs4 import BeautifulSoup as bs4
from bs4.element import Tag
from bs4.element import ResultSet
from datetime import datetime
from dateutil.parser import parse
from common_lib.common_settings import TIMEZONE
import time
import requests


def scraper(soup: bs4, scraper:str, scrape_parm: list[dict[str, str]]) -> tuple[dict, dict]:
    ''' '''
    scraped_result: dict = {}
    scraped_pattern: dict = {}
    scraped_item = None
    scrape_info: dict = {}
    ### cssセレクターでスクレイプ対象を取得できるまで繰り返し ###
    for scrape_info in scrape_parm:
        scraped_item = soup.select(scrape_info['css_selecter'])
        if type(scraped_item) is ResultSet:
            scraped_pattern = {scraper: scrape_info['pattern']}
            item_text: list[str] = [tag.get_text() for tag in scraped_item]
            scraped_result['article'] = '\n'.join(item_text).strip()
            break

    return scraped_result, scraped_pattern


if __name__ == '__main__':
    '''単体テスト用の設定'''
    test_url = 'https://www.yomiuri.co.jp/world/20220401-OYT1T50197/'
    #test_url = 'https://www.sankei.com/article/20220402-4DJ4CTA7MBKVHIH7KX3CA2FYGY/'
    # 通常サイト用
    request = requests.get(test_url)
    # JavaScript動作後の取得用。Splashを利用。
    # request = requests.get('http://localhost:8050/render.html',
    #                       params={'url': test_url, 'wait': 0.5})
    # bs4で解析
    soup: bs4 = bs4(request.text, 'lxml')

    # scrape用のパラメータ

    # body > div#fusion-app > div.grid div.grid > section > article.article-wrapper > div.article-body > p.article-text
    # DB内のデータイメージ
    {'domain': 'yomiuri.co.jp',
     'scrape_item': {
        'article_scraper': [
            {'pattern': 2,
                'css_selecter': 'div.p-main-contents > p[iarticle_selecterrop=articleBody]'},
            {'pattern': 1,
                'css_selecter': 'div.main-contents > p[iarticle_selecterrop=articleBody]'},
        ]}}
    # パターン: pattern
    # 型：str,datetime      type
    # 数：single,multipule  single_or_multiple

    scrape_parm = [
        {'pattern': 4,
         'css_selecter': 'div.p-main-contents > p'},
        {'pattern': 3,
         'css_selecter': 'div.p-main-contents > p[class^=par]'},
        {'pattern': 2,
         'css_selecter': 'div.p-main-contents > p[iarticle_selecterrop=articleBody]'},
        {'pattern': 1,
         'css_selecter': 'div.main-contents > p[iarticle_selecterrop=articleBody]'},
    ]
    scrape_parm = sorted(scrape_parm, key=lambda d: d['pattern'], reverse=True)
    print('\n\n=== scrape_parm ===', scrape_parm)

    result = scraper(
        soup=soup,
        scraper='article_scraper',
        scrape_parm=scrape_parm,
    )
    print('\n\n=== result ===', result)
