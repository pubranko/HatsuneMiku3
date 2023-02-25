import re
import urllib.parse
import pickle
from datetime import datetime
from time import sleep
from typing import Pattern, Any
from bs4.element import ResultSet
from urllib.parse import unquote
from bs4 import BeautifulSoup as bs4
import scrapy
from scrapy.http import Response
from scrapy.http import TextResponse
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
from scrapy_selenium import SeleniumRequest
from scrapy.exceptions import CloseSpider
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate
from news_crawl.spiders.common.urls_continued_skip_check import UrlsContinuedSkipCheck
from news_crawl.items import NewsCrawlItem
from news_crawl.spiders.common.url_pattern_skip_check import url_pattern_skip_check
from shared.login_info_get import login_info_get


class NikkeiComCrawlSpider(ExtensionsCrawlSpider):
    name: str = 'nikkei_com_crawl'
    allowed_domains: list = ['nikkei.com']
    start_urls: list = [
        'https://www.nikkei.com/news/category/',     # 新着
        # 'https://www.nikkei.com/news/category/?bn=1',  #クエリー部分で取得開始したい記事を指定。省略すればbn=1として処理される。
        # 'https://www.nikkei.com/news/category/?bn=',  # 初期処理で指定ページに合わせてbn=部をカスタマイズ
    ]
    _domain_name: str = 'nikkei_com'         # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    custom_settings: dict = {
        'DEPTH_LIMIT': 0,
        'DEPTH_STATS_VERBOSE': True,
    }

    # rules = (
    #     Rule(LinkExtractor(
    #         allow=(r'/article/')), callback='parse_news'),
    # )

    # seleniumモード
    # selenium_mode: bool = True

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

        self.pages: dict = self.pages_setting(1, 2)
        self.start_page: int = self.pages['start_page']
        self.end_page: int = self.pages['end_page']
        self.page: int = self.start_page
        self.all_urls_list: list = []
        self.url_continued = UrlsContinuedSkipCheck(
            self._crawl_point, self.start_urls[0], self.kwargs_save)

    def start_requests(self):
        ''' '''
        ### start_urlsをベースにページに合わせたurlを生成
        # 例）https://www.nikkei.com/news/category/
        #     -> https://www.nikkei.com/news/category/?bn=5,
        # 記事の開始位置をページより計算して求める。
        yield scrapy.Request(
            url=f'{self.start_urls[0]}?bn={(self.start_page * 30) - 29}',
            callback=self.parse_start_response)

    def parse_start_response(self, response: TextResponse):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み
        '''
        while self.page <= self.end_page:
            self.logger.info(
                f'=== parse_start_response 現在解析中のURL = {response.url}')

            # ページ内の対象urlを抽出
            #CONTENTS_MAIN > div:nth-child(1) > ul > li:nth-child(1) > h3 > span > span.m-miM32_itemTitleText > a
            #CONTENTS_MAIN > div > ul > li > h3 > span > span.m-miM32_itemTitleText > a[href]
            #CONTENTS_MAIN > div > h3 > a[href]

            #CONTENTS_MAIN > div > h3.m-miM09_title > a[href]::attr(href)
            #CONTENTS_MAIN > div > ul > li > h3 > span > span.m-miM32_itemTitleText > a[href]

            links = response.css(
                f'#CONTENTS_MAIN > div > h3.m-miM09_title > a[href]::attr(href)').getall()
            links.extend(response.css(
                f'#CONTENTS_MAIN > div > ul > li > h3 > span > span.m-miM32_itemTitleText > a[href]::attr(href)').getall())
            self.logger.info(
                f'=== ページ内の記事件数 = {len(links)}')
            # ページ内記事は通常30件。それ以外の場合はワーニングメール通知（環境によって違うかも、、、）
            if not len(links) == 30:
                self.logger.warning(
                    f'=== parse_start_response 1ページ内で取得できた件数が想定の30件と異なる。確認要。 ( {len(links)} 件)')

            for link in links:
                # 相対パスの場合絶対パスへ変換。また%エスケープされたものはUTF-8へ変換
                url: str = urllib.parse.unquote(response.urljoin(link))
                self.all_urls_list.append({'loc': url, 'lastmod': ''})
                # 前回からの続きの指定がある場合、
                # 前回取得したurlが確認できたら確認済み（削除）にする。

                if self.url_continued.skip_check(url):
                    pass
                elif url_pattern_skip_check(url, self.kwargs_save):
                    pass
                else:
                    # クロール対象のURL情報を保存
                    self.crawl_urls_list.append(
                        {'loc': url, 'lastmod': '', 'source_url': response.url})
                    self.crawl_target_urls.append(url)

            # debug指定がある場合、現ページの３０件をデバック用ファイルに保存
            start_request_debug_file_generate(
                self.name, response.url, self.all_urls_list[-30:], self.kwargs_save)

            # 前回からの続きの指定がある場合、前回の10件のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
            if self.url_continued.skip_flg == True:
                self.logger.info(
                    f'=== parse_start_response 前回の続きまで再取得完了 ({response.url})', )
                self.page = self.end_page + 1
                break

            # 次のページを読み込む => start_urlsに必要ページのurlを設定済み
            self.page += 1
            if self.page <= self.end_page:
                # 要素を表示するようスクロールしてクリック
                next_page_url = f'{self.start_urls[0]}?bn={(self.start_page * 30) - 29}'
                yield scrapy.Request(url=next_page_url, callback=self.parse_start_response)

        # リスト(self.urls_list)に溜めたurlをリクエストへ登録する。
        for _ in self.crawl_urls_list:
            yield scrapy.Request(response.urljoin(_['loc']), callback=self.parse_news,)
        # 次回向けに1ページ目の10件をcontrollerへ保存する
        self._crawl_point[self.start_urls[0]] = {
            'urls': self.all_urls_list[0:self.url_continued.check_count],
            'crawling_start_time': self._crawling_start_time
        }
