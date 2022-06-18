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
from news_crawl.spiders.common.lua_script_get import lua_script_get
from news_crawl.spiders.common.urls_continued_skip_check import UrlsContinuedSkipCheck
from news_crawl.spiders.common.url_pattern_skip_check import url_pattern_skip_check


class JpReutersComCrawlSpider(ExtensionsCrawlSpider):
    name: str = 'jp_reuters_com_crawl'
    allowed_domains: list = ['jp.reuters.com']
    start_urls: list = [
        # 'https://jp.reuters.com/news/archive?view=page&page=1&pageSize=10'  # 最新ニュース
        # 'https://jp.reuters.com/news/archive?view=page&page=2&pageSize=10' #2ページ目
    ]
    _domain_name: str = 'jp_reuters_com'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    custom_settings: dict = {
        'DEPTH_LIMIT': 0,
        'DEPTH_STATS_VERBOSE': True,
        'DOWNLOADER_MIDDLEWARES': {
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

        # 開始ページからURLを生成
        url = f'https://jp.reuters.com/news/archive?view=page&page={self.pages["start_page"]}&pageSize=10'
        self.start_urls.append(url)
        # https://jp.reuters.com/news/archive?view=page&page=1&pageSize=10  →  https://jp.reuters.com/news/archive を抽出
        _ = str(url).split('?')[0]
        # keyにドット(.)があるとエラーMongoDBがエラーとなるためアンダースコアに置き換え
        self.base_url = _.replace('.', '_')

        self.url_continued = UrlsContinuedSkipCheck(
            self._crawl_point, self.base_url, self.kwargs_save)

    def start_requests(self):
        ''' '''
        if self.selenium_mode:
            for url in self.start_urls:
                yield SeleniumRequest(
                    url=url,
                    callback=self.parse_start_response_selenium)

        elif self.splash_mode:
            for url in self.start_urls:
                yield SplashRequest(
                    url=url,
                    callback=self.parse_start_response_splash,
                    meta={'max_retry_times': 20},
                    endpoint='execute',
                    cache_args=['lua_source'],
                    args={
                        'lua_source': lua_script_get('first_load'),
                        'find_element': 'div.control-nav > a.control-nav-next',  # 左記の要素が表示されるまで待機させる。
                    },
                    headers={'X-My-Header': 'value'},
                    session_id=self.session_id)  # 任意の値

    def parse_start_response_selenium(self, response: TextResponse):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み(selenium版)
        '''
        r: Any = response.request
        driver: WebDriver = r.meta['driver']

        while self.page <= self.end_page:
            self.logger.info(
                f'=== parse_start_response 現在解析中のURL = {driver.current_url}')
            driver.set_page_load_timeout(5)
            # driver.implicitly_wait(15)
            driver.set_script_timeout(5)

            next_page_element = f'div.control-nav > a.control-nav-next[href="?view=page&page={self.page + 1}&pageSize=10"]'
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, next_page_element)))

            # ページ内の対象urlを抽出
            _ = driver.find_elements_by_css_selector('.story-content a[href]')
            links: list = [link.get_attribute("href") for link in _]
            self.logger.info(
                f'=== ページ内の記事件数 = {len(links)}')
            # ページ内記事は通常10件。それ以外の場合はワーニングメール通知（環境によって違うかも、、、）
            if not len(links) == 10:
                self.logger.warning(
                    f'=== parse_start_response 1ページ内で取得できた件数が想定の10件と異なる。確認要。 ( {len(links)} 件)')

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
                        {'loc': url, 'lastmod': '', 'source_url': driver.current_url})
                    self.crawl_target_urls.append(url)

            # debug指定がある場合、現ページの１０件をデバック用ファイルに保存
            start_request_debug_file_generate(
                self.name, driver.current_url, self.all_urls_list[-10:], self.kwargs_save)

            # 前回からの続きの指定がある場合、前回の5件のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
            if self.url_continued.skip_flg == True:
                self.logger.info(
                    f'=== parse_start_response 前回の続きまで再取得完了 ({driver.current_url})', )
                self.page = self.end_page + 1
                break

            # 次のページを読み込む
            self.page += 1
            elem: WebElement = driver.find_element_by_css_selector(
                'div.control-nav > a.control-nav-next')
            elem.click()

        # リスト(self.urls_list)に溜めたurlをリクエストへ登録する。
        for _ in self.crawl_urls_list:
            yield scrapy.Request(response.urljoin(_['loc']), callback=self.parse_news,)
        # 次回向けに1ページ目の5件をcontrollerへ保存する
        self._crawl_point[self.base_url] = {
            'urls': self.all_urls_list[0:self.url_continued.check_count],
            'crawling_start_time': self._crawling_start_time
        }

    def parse_start_response_splash(self, response: SplashJsonResponse):
        ''' (拡張メソッド)
        取得したレスポンスよりDBへ書き込み(splash版)
        '''
        self.logger.info(
            f'=== parse_start_response 現在解析中のURL = {response.url}')

        # ページ内の対象urlを抽出
        links: list = response.css(
            '.story-content a[href]::attr(href)').getall()
        self.logger.info(
            f'=== ページ内の記事件数 = {len(links)}')
        # ページ内記事は通常10件。それ以外の場合はワーニングメール通知（環境によって違うかも、、、）
        if not len(links) == 10:
            self.logger.warning(
                f'=== parse_start_response 1ページ内で取得できた件数が想定の10件と異なる。確認要。 ( {len(links)} 件)')

        for link in links:
            url: str = urllib.parse.unquote(response.urljoin(link))
            self.all_urls_list.append({'loc': url, 'lastmod': ''})
            # 前回からの続きの指定がある場合、
            # 前回取得したurlが確認できたら確認済み（削除）にする。
            if self.url_continued.skip_check(url):
                pass
            elif url_pattern_skip_check(url, self.kwargs_save):
                pass
            else:
                self.crawl_urls_list.append(
                    {'loc': url, 'lastmod': '', 'source_url': response.url})

        # 前回からの続きの指定がある場合、前回の5件のurlが全て確認できたら前回以降に追加された記事は全て取得完了と考えられるため終了する。
        if self.url_continued.skip_flg == False:
            self.logger.info(
                f'=== parse_start_response 前回の続きまで再取得完了 ({response.url})', )
            self.page = self.end_page + 1

        start_request_debug_file_generate(
            self.name, response.url, self.all_urls_list[-10:], self.kwargs_save)

        # 次のページを読み込む
        self.page += 1
        next_page_element = 'div.control-nav > a.control-nav-next[href="?view=page&page=' + str(
            self.page + 1) + '&pageSize=10"]'
        click_element = 'div.control-nav > a.control-nav-next'
        if self.page <= self.end_page:
            yield SplashRequest(
                url=response.url,
                callback=self.parse_start_response_splash,
                endpoint='execute',
                cache_args=['lua_source'],
                args={
                    'lua_source': lua_script_get('click'),
                    'click_element': click_element,  # 左記の要素をクリックする
                    # クリック後、左記の要素が表示されるまで待機させる。(現ページ＋２)
                    'find_element': next_page_element,
                },
                headers={'X-My-Header': 'value'},
                session_id=self.session_id,
            )
        else:
            # リスト(self.urls_list)に溜めたurlをリクエストへ登録する。
            for _ in self.crawl_urls_list:
                yield scrapy.Request(response.urljoin(_['loc']), callback=self.parse_news,)
            # 次回向けに1ページ目の5件をcontrollerへ保存する
            self._crawl_point[self.base_url] = {
                'urls': self.all_urls_list[0:self.url_continued.check_count],
                'crawling_start_time': self._crawling_start_time
            }
