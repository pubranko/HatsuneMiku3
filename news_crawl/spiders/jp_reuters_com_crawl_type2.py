from typing import Any, Type
from news_crawl.spiders.extensions_crawl import ExtensionsCrawlSpider
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
from scrapy.http import Response
from scrapy.http.response.html import HtmlResponse
from scrapy_selenium import SeleniumRequest
#from scrapy import statscollectors
from news_crawl.items import NewsCrawlItem
from datetime import datetime
import pickle
import os
import re
import scrapy
from time import sleep
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup as bs4
from bs4.element import ResultSet
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.webelement import FirefoxWebElement
import urllib.parse


class JpReutersComCrawlType2Spider(ExtensionsCrawlSpider):
    name: str = 'jp_reuters_com_crawl_type2'
    allowed_domains: list = ['jp.reuters.com']
    start_urls: list = [
        'https://jp.reuters.com/news/archive?view=page&page=1&pageSize=10'  # 最新ニュース
        # 'https://jp.reuters.com/news/archive?view=page&page=2&pageSize=10' #2ページ目
    ]
    _domain_name: str = 'jp_reuters_com_crawl'        # 各種処理で使用するドメイン名の一元管理
    spider_version: float = 1.0

    # start_urlsまたはstart_requestの数。起点となるurlを判別するために使う。
    _crawl_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _crawl_next_info: dict = {name: {}, }

    rules = (
        Rule(LinkExtractor(
            allow=(r'/article/')), callback='parse_news'),
    )

    def start_requests(self):
        '''start_urlsを使わずに直接リクエストを送る。
        あとで
        '''
        self._crawl_urls_count += 1
        yield SeleniumRequest(
            url='https://jp.reuters.com/news/archive?view=page&page=1&pageSize=10',
            callback=self.parse_start_response,
        )

    def parse_start_response(self, response: HtmlResponse):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        driver: FirefoxWebElement = response.request.meta['driver']
        # クリック対象が読み込み完了していることを確認
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, '.control-nav-next'))
        )

        # ループ条件
        # 1.現在のページ数は、10ページまで（仮）
        # 2.前回の1ページ目の記事リンク（10件）まで全て遡りきったら、前回以降に追加された記事は取得完了と考えられるため終了。

        # 現在のページより次ページのURLを取得
        element: FirefoxWebElement = driver.find_element_by_css_selector(
            '.control-nav-next')
        next_page_url: str = element.get_attribute("href")
        print('=== next_page_url = ', next_page_url)
        # 次のページのページ数を取得
        _ = re.compile('https://jp.reuters.com/news/archive\?view=page&page=')
        temp: str = _.sub('', next_page_url)
        _ = re.compile('&pageSize=10')
        page = _.sub('', temp)
        print('===次のページ数 = ', page)

        # 現在のページ内の記事のリンクをリストへ保存
        urls_list: list = []
        links: list = driver.find_elements_by_css_selector(
            '.story-content a')
        for link in links:
            link: FirefoxWebElement
            url: str = urllib.parse.unquote(link.get_attribute("href"))
            urls_list.append(url)
            print('=== ページ内対象リンク = ', url)

        # リストにためたurlをリクエストへ登録する。
        for url in urls_list:
            yield scrapy.Request(response.urljoin(url), callback=self.parse_news)
        # 次回向けに1ページ目の10件をcrawler_controllerへ保存する情報
        self._crawl_next_info[self.name][self.start_urls[self._crawl_urls_count]] = {
            'urls': urls_list[0:10],
            'crawl_start_time': self._crawl_start_time_iso,
        }


        self.common_prosses(self.start_urls[self._crawl_urls_count], response)

        # yield scrapy.Request(response.urljoin(href), self.parse_news)

        _info = self.name + ':' + str(self.spider_version) + ' / ' \
            + 'extensions_crawl:' + str(self._extensions_crawl_version)
        # yield NewsCrawlItem(
        #     url=response.url,
        #     response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
        #     response_headers=pickle.dumps(response.headers),
        #     response_body=pickle.dumps(response.body),
        #     spider_version_info=_info
        # )
