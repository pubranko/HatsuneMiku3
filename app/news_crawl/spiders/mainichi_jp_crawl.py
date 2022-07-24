import urllib.parse
import scrapy
from datetime import datetime
import urllib.parse
from typing import Any
from scrapy.http import TextResponse
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import Rule
from scrapy_splash import SplashRequest
from scrapy_splash.response import SplashJsonResponse
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
from news_crawl.spiders.common.start_request_debug_file_generate import start_request_debug_file_generate
from news_crawl.spiders.common.urls_continued_skip_check import UrlsContinuedSkipCheck
from news_crawl.spiders.common.url_pattern_skip_check import url_pattern_skip_check

class MainichiJpCrawlSpider(ExtensionsCrawlSpider):
    name: str = 'mainichi_jp_crawl'
    allowed_domains: list = ['mainichi.jp']
    start_urls: list = [
        'https://mainichi.jp/flash/',   # ピックアップ、新着
    ]
    _domain_name: str = 'mainichi_jp'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    custom_settings: dict = {
        'DEPTH_LIMIT': 0,
        'DEPTH_STATS_VERBOSE': True,
        'DOWNLOADER_MIDDLEWARES' : {
            'news_crawl.scrapy_selenium_custom_middlewares.SeleniumMiddleware': 585,
        },
    }

    # rules = (
    #     Rule(LinkExtractor(
    #         allow=(r'/article/')), callback='parse_news'),
    # )
    # seleniumモード
    selenium_mode: bool = True
    # splashモード
    #splash_mode: bool = True

    def __init__(self, *args, **kwargs):
        ''' (拡張メソッド)
        親クラスの__init__処理後に追加で初期処理を行う。
        '''
        super().__init__(*args, **kwargs)

        self.pages: dict = self.pages_setting(1, 3)
        self.start_page: int = self.pages['start_page']
        self.end_page: int = self.pages['end_page']
        self.page: int = self.start_page
        self.all_urls_list: list = []
        self.session_id: str = self.name + datetime.now().isoformat()

        # keyにドット(.)があるとエラーMongoDBがエラーとなるためアンダースコアに置き換え
        self.base_url = str(self.start_urls[0]).replace('.', '_')

        self.url_continued = UrlsContinuedSkipCheck(
            self._crawl_point, self.base_url, self.kwargs_save)

    def start_requests(self):
        ''' '''
        if self.selenium_mode:
            for url in self.start_urls:
                yield SeleniumRequest(
                    url=url,
                    callback=self.parse_start_response_selenium)

    def parse_start_response_selenium(self, response: TextResponse):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み(selenium版)
        '''
        r: Any = response.request
        driver: WebDriver = r.meta['driver']

        number_of_details_in_page:int = 20  # 1ページ内の明細数

        while self.page <= self.end_page:
            self.logger.info(
                f'=== parse_start_response_selenium 現在解析中のURL = {driver.current_url}')
            driver.set_page_load_timeout(60)
            driver.set_script_timeout(60)

            target_article_element = f'#article-list > ul > li:nth-child({number_of_details_in_page * self.page})'
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, target_article_element)))

            # ページ内の対象urlを抽出
            _ = driver.find_elements_by_css_selector('#article-list > ul > li > a[href]')
            links: list = [link.get_attribute("href") for link in _]
            self.logger.info(
                f'=== ページ内の記事件数 = {len(links)}')
            # 1ページ内の明細数以外の場合はワーニングメール通知（環境によって違うかも、、、）
            if not len(links) == number_of_details_in_page * self.page:
                self.logger.warning(
                    f'=== parse_start_response_selenium ページ内で取得できた件数が想定の{number_of_details_in_page * self.page}件と異なる。確認要。 ( {len(links)} 件)')

            for link in links:
                # crwal_flg = True
                # 相対パスの場合絶対パスへ変換。また%エスケープされたものはUTF-8へ変換
                url: str = urllib.parse.unquote(response.urljoin(link))
                self.all_urls_list.append({'loc': url, 'lastmod': ''})

                # if url_pattern_skip_check(url, self.kwargs_save):
                #    crwal_flg = False
                # if self.url_continued.skip_check(url):
                #     pass
                # else:
                #     crwal_flg = False

                # 前回からの続きの指定がある場合、
                # 前回取得したurlまで確認できたらそれ移行は対象外
                if self.url_continued.skip_check(url):
                    pass
                # urlパターンの絞り込みで対象外となった場合
                elif url_pattern_skip_check(url, self.kwargs_save):
                    pass
                else:
                # if crwal_flg:
                    # クロール対象のURL情報を保存
                    self.crawl_urls_list.append(
                        {'loc': url, 'lastmod': '', 'source_url': driver.current_url})
                    self.crawl_target_urls.append(url)

            # debug指定がある場合、現ページの明細数分をデバック用ファイルに保存
            start_request_debug_file_generate(
                self.name, driver.current_url, self.all_urls_list[-number_of_details_in_page:], self.kwargs_save)

            # 前回からの続きの指定がある場合、前回の5件のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
            if self.url_continued.skip_flg == False:
                self.logger.info(
                    f'=== parse_start_response_selenium 前回の続きまで再取得完了 ({driver.current_url})', )
                self.page = self.end_page + 1
                break

            # 次のページを読み込む
            self.page += 1
            elem: WebElement = driver.find_element_by_css_selector(
                'div.main-contents span.link-more')
            elem.location_once_scrolled_into_view   # 要素までスクロールを移動してボタンをウィンドウに表示させる。
            elem.click()

        # リスト(self.urls_list)に溜めたurlをリクエストへ登録する。
        for _ in self.crawl_urls_list:
            yield scrapy.Request(response.urljoin(_['loc']), callback=self.parse_news,)
        # 次回向けに1ページ目の5件をcontrollerへ保存する
        self._crawl_point[self.base_url] = {
            'urls': self.all_urls_list[0:self.url_continued.check_count],
            'crawling_start_time': self._crawling_start_time
        }
