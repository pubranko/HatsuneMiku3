from typing import Any, Type
from news_crawl.spiders.extensions_crawl import ExtensionsCrawlSpider
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
from scrapy.http import Response
from scrapy.http.response.html import HtmlResponse
from scrapy_selenium import SeleniumRequest
from news_crawl.items import NewsCrawlItem
from datetime import datetime
import pickle,os,scrapy
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

class JpReutersComCrawlSpider(ExtensionsCrawlSpider):
    name:str = 'jp_reuters_com_crawl'
    allowed_domains:list = ['jp.reuters.com']
    start_urls:list = [
         #'http://jp.reuters.com/',
         'https://jp.reuters.com/theWire',
         #'https://jp.reuters.com/news/topNews',
         ]
    _domain_name: str = 'jp_reuters_com_crawl'        # 各種処理で使用するドメイン名の一元管理
    spider_version: float = 1.0

    # sitemap_urlsに複数のサイトマップを指定した場合、その数だけsitemap_filterが可動する。その際、どのサイトマップか判別できるように処理中のサイトマップと連動するカウント。
    _crawl_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _cwawl_next_info: dict = {name: {}, }

    rules = (
        #Rule(LinkExtractor(
        #    allow=(r'/theWire')), callback='parse_top_news'),
        Rule(LinkExtractor(
            allow=(r'/article/')), callback='parse_news'),
    )

    def start_requests(self):
        '''start_urlsを使わずに直接リクエストを送る。
        あとで
        '''
        # yield scrapy.Request('http://www.example.com/1.html', self.parse)
        # yield scrapy.Request('http://www.example.com/2.html', self.parse)
        # yield scrapy.Request('http://www.example.com/3.html', self.parse)
        # print('=== web driver start ===',datetime.now())
        # options = Options()
        # options.set_headless()
        # driver = webdriver.Firefox(options=options)

        # driver.get("https://jp.reuters.com/theWire")
        # print('=== web driver 2 ===',datetime.now())
        # driver.find_element_by_css_selector('.load-more-link').click()
        # print('=== web driver 3 ===',datetime.now())
        # element = WebDriverWait(driver, 10).until(
        #     EC.presence_of_element_located((By.CLASS_NAME, 'load-more-link'))
        # )
        # print('=== web driver 4 ===',datetime.now())
        # links = driver.find_element_by_class_name('article')
        # print(type(links))
        # _ = 0
        # for link in links:
        #     _ += 1
        # print(_)

        # print('=== web driver 5 ===',datetime.now())
        #print(element)

        yield SeleniumRequest(
            url='https://jp.reuters.com/theWire',
            callback=self.parse_start_response,
            #callback=self.parse,
            wait_time=10,
            wait_until=EC.element_to_be_clickable((By.CLASS_NAME, 'load-more-link')),
            #screenshot=True,
            )

        ### class="load-more-link
        # <div class="load-more-link load-more-wire wire-container">
        # <span class="load-more-content active">
        #     <div class="more-load">LOAD MORE</div>
        #     <div class="more-arrow">∨</div>
        # </span>
        # <div class="loader-container loader-wire">
        #     <div class="loader"><div class="loader-dot"></div><div class="loader-dot"></div><div class="loader-dot"></div></div>
        # </div>
        # </div>
        #yield SeleniumRequest('https://jp.reuters.com/news/topNews',self.parse_top_news,)
        #yield SeleniumRequest('https://jp.reuters.com/news/topNews',self.parse_top_news,)
        '''
        (class) SeleniumRequest(
            wait_time: Unknown = None,
            wait_until: Unknown = None,
            screenshot: Unknown = False,
            script: Unknown = None,
            *args: Unknown, **kwargs: Unknown)
        これらを使用すると、seleniumはスパイダーにレスポンスを返す前に 明示的なウエイトを実行します。
        yield SeleniumRequest(
            url=url,
            callback=self.parse_result,
            wait_time=10,
            wait_until=EC.element_to_be_clickable((By.ID, 'someid'))
        )
        これを使用すると、seleniumはページのスクリーンショットを撮り、キャプチャしたPNG画像のバイナリデータが response meta に追加されます。
        yield SeleniumRequest(
           url=url,
            callback=self.parse_result,
            screenshot=True
        )
        これを使用すると、seleniumはカスタムJavaScriptコードを実行します。
        yield SeleniumRequest(
           url=url,
            callback=self.parse_result,
            script='window.scrollTo(0, document.body.scrollHeight);',
        )
        '''


    # def parse_start_url(self, response: Response):
    #     '''
    #     start_urls自体のレスポンスの処理
    #     '''
    #     self.common_prosses(self.start_urls[self._crawl_urls_count], response)
    #     self._crawl_urls_count += 1  # 次のurl用にカウントアップ



    def parse_start_response(self, response: HtmlResponse):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        print('=== parse_start_response ===')
        driver:FirefoxWebElement = response.request.meta['driver']

        driver.find_element_by_css_selector('.load-more-link').click()
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '.load-more-content.active'))  #クリック後のajax完了まで待つ
        )
        articles = driver.find_elements_by_css_selector('li>h3.article-heading>a')

        cnt = 0
        for art in articles:
            url = urllib.parse.unquote(art.get_attribute('href'))
            #yield scrapy.Request(response.urljoin(url), self.parse_news)
            yield scrapy.Request(response.urljoin(url))
            print(url)
            print(art.text)
            cnt += 1
        print('=== cnt = ',cnt)

        self.common_prosses(self.start_urls[self._crawl_urls_count], response)

        #yield scrapy.Request(response.urljoin(href), self.parse_news)

        _info = self.name + ':' + str(self.spider_version) + ' / ' \
            + 'extensions_crawl:' + str(self._extensions_crawl_version)
        # yield NewsCrawlItem(
        #     url=response.url,
        #     response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
        #     response_headers=pickle.dumps(response.headers),
        #     response_body=pickle.dumps(response.body),
        #     spider_version_info=_info
        # )
