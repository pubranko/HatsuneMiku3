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


class JpReutersComCrawlType1Spider(ExtensionsCrawlSpider):
    name: str = 'jp_reuters_com_crawl_type1'
    allowed_domains: list = ['jp.reuters.com']
    start_urls: list = [
        # 'http://jp.reuters.com/',
        'https://jp.reuters.com/theWire',
        # 'https://jp.reuters.com/news/topNews',
        # 'https://jp.reuters.com/news/archive?view=page&page=1&pageSize=10' #最新ニュース
        # 'https://jp.reuters.com/news/archive?view=page&page=2&pageSize=10' #2ページ目
    ]
    _domain_name: str = 'jp_reuters_com_crawl'        # 各種処理で使用するドメイン名の一元管理
    spider_version: float = 1.0

    # sitemap_urlsに複数のサイトマップを指定した場合、その数だけsitemap_filterが可動する。その際、どのサイトマップか判別できるように処理中のサイトマップと連動するカウント。
    _crawl_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _cwawl_next_info: dict = {name: {}, }

    rules = (
        # Rule(LinkExtractor(
        #    allow=(r'/theWire')), callback='parse_top_news'),
        Rule(LinkExtractor(
            allow=(r'/article/')), callback='parse_news'),
    )

    # def parse_start_url(self, response: Response):
    #     '''
    #     start_urls自体のレスポンスの処理
    #     '''
    #     self.common_prosses(self.start_urls[self._crawl_urls_count], response)
    #     self._crawl_urls_count += 1  # 次のurl用にカウントアップ

    def start_requests(self):
        '''start_urlsを使わずに直接リクエストを送る。
        あとで
        '''
        # yield scrapy.Request('http://www.example.com/1.html', self.parse)
        # yield scrapy.Request('http://www.example.com/2.html', self.parse)
        # yield SeleniumRequest('https://jp.reuters.com/news/topNews',self.parse_top_news,)
        # yield SeleniumRequest('https://jp.reuters.com/news/topNews',self.parse_top_news,)
        yield SeleniumRequest(
            url='https://jp.reuters.com/theWire',
            callback=self.parse_start_response,
            # wait_time=10,
            # wait_until=EC.element_to_be_clickable(
            #     (By.CSS_SELECTOR, '.load-more-link')),
        )

    def parse_start_response(self, response: HtmlResponse):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        driver: FirefoxWebElement = response.request.meta['driver']

        # クリック対象が読み込み完了していることを確認
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, '.load-more-content.active'))
        )
        articles = driver.find_elements_by_css_selector(
            'li>h3.article-heading>a')
        self.logger.info(
            '=== parse_start_response : 記事数 = %s', len(articles))

        driver.find_element_by_css_selector('.load-more-link').click()

        # クリック後のajax完了まで待つ
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, '.load-more-content.active'))
        )
        articles = driver.find_elements_by_css_selector(
            'li>h3.article-heading>a')
        self.logger.info(
            '=== parse_start_response : 記事数 = %s', len(articles))

        for art in articles:
            url = urllib.parse.unquote(art.get_attribute('href'))
            yield scrapy.Request(response.urljoin(url), callback=self.parse_news)


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
