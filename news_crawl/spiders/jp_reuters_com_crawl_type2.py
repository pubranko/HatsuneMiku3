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
import urllib.parse
import scrapy
from bs4 import BeautifulSoup as bs4
from bs4.element import ResultSet
from time import sleep
#from selenium import webdriver
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.webelement import FirefoxWebElement

class JpReutersComCrawlType2Spider(ExtensionsCrawlSpider):
    name: str = 'jp_reuters_com_crawl_type2'
    allowed_domains: list = ['jp.reuters.com']
    start_urls: list = [
        # 'https://jp.reuters.com/news/archive?view=page&page=1&pageSize=10'  # 最新ニュース
        # 'https://jp.reuters.com/news/archive?view=page&page=2&pageSize=10' #2ページ目
    ]
    _domain_name: str = 'jp_reuters_com'        # 各種処理で使用するドメイン名の一元管理
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
        # 開始ページからURLを生成
        pages:dict = self.pages_setting(1,3)
        start_page: int = pages['start_page']
        url='https://jp.reuters.com/news/archive?view=page&page=' + str(start_page) + '&pageSize=10'

        self.start_urls.append(url)

        yield SeleniumRequest(
            url=url,
            callback=self.parse_start_response,
        )

    def parse_start_response(self, response: HtmlResponse):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        # ループ条件
        # 1.現在のページ数は、10ページまで（仮）
        # 2.前回の1ページ目の記事リンク（10件）まで全て遡りきったら、前回以降に追加された記事は取得完了と考えられるため終了。

        pages:dict = self.pages_setting(1,3)
        start_page: int = pages['start_page']
        end_page: int = pages['end_page']

        driver: WebDriver = response.request.meta['driver']
        urls_list: list = []

        # 前回からの続きの指定がある場合、前回の１ページ目の１０件のURLを取得する。
        last_time_urls:list = []
        if 'continued' in self.kwargs_save:
            last_time_urls:list = self._crawler_controller_recode[self.name][self.start_urls[self._crawl_urls_count]]['urls']
            print('=== continuedで動くよ〜')

        self._crawler_controller_recode

        while start_page <= end_page:
            self.logger.info(
                '=== parse_start_response 現在処理中のURL = %s', driver.current_url)

            # クリック対象が読み込み完了していることを確認   例）href="?view=page&amp;page=2&amp;pageSize=10"
            start_page += 1    #次のページ数
            next_page_selecter: str = '.control-nav-next[href$="view=page&page=' + \
                str(start_page) + '&pageSize=10"]'
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, next_page_selecter))
            )

            # 現在のページ内の記事のリンクをリストへ保存
            links: list = driver.find_elements_by_css_selector(
                '.story-content a')
            for link in links:
                link: WebElement
                url: str = urllib.parse.unquote(link.get_attribute("href"))
                urls_list.append(url)

                # 前回取得したurlが確認できたら確認済み（削除）にする。
                if url in last_time_urls:
                    last_time_urls.remove(url)

            # 前回からの続きの指定がある場合、前回の１ページ目のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
            if 'continued' in self.kwargs_save:
                print('=== continuedの結果確認〜',len(last_time_urls))
                if len(last_time_urls) == 0:
                    self.logger.info(
                        '=== parse_start_response 前回の続きまで再取得完了 (%s)', driver.current_url)
                    break

            # 次のページを読み込む
            elem:WebElement = driver.find_element_by_css_selector('.control-nav-next')
            elem.click()

        # リストに溜めたurlをリクエストへ登録する。
        for url in urls_list:
            yield scrapy.Request(response.urljoin(url), callback=self.parse_news)
        # 次回向けに1ページ目の10件をcrawler_controllerへ保存する情報
        self._crawl_next_info[self.name][self.start_urls[self._crawl_urls_count]] = {
            'urls': urls_list[0:10],
            'crawl_start_time': self._crawl_start_time_iso,
        }

        self.common_prosses(self.start_urls[self._crawl_urls_count], urls_list)

        self._crawl_urls_count += 1
